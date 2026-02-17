/**
 * gRPC server that emulates Firestore API
 */

import * as fs from 'fs';
import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protobuf from 'protobufjs';
import { getLogger, Logger } from '../logger';
import {
  FirestoreDocument,
  FirestoreValue,
  ServerConfig,
  FieldType,
} from '../types';
import { handleBatchGetDocuments } from './handlers/batchGetDocuments';
import { handleCommitWithProtobufjs } from './handlers/commit';
import { handleCreateDocument } from './handlers/createDocument';
import { handleDeleteDocument } from './handlers/deleteDocument';
import { handleGetDocument } from './handlers/getDocument';
import { handleListCollectionIds } from './handlers/listCollectionIds';
import { handleListDocuments } from './handlers/listDocuments';
import { handleListen } from './handlers/listen';
import { handleRunAggregationQuery } from './handlers/runAggregationQuery';
import { handleRunQuery } from './handlers/runQuery';
import { handleUnimplementedUnary } from './handlers/unimplementedUnary';
import { handleUpdateDocument } from './handlers/updateDocument';
import { handleWrite } from './handlers/write';
import { FirestoreStorage } from './storage';
import * as firestoreUtils from './utils';

export class FirestoreServer {
  private readonly storage: FirestoreStorage;
  private readonly config: ServerConfig;
  private grpcServer?: grpc.Server;
  public readonly logger: Logger = getLogger();
  public protobufRoot?: protobuf.Root;
  // Message types for manual deserialization with protobufjs
  private CommitRequestType?: protobuf.Type;
  private ValueType?: protobuf.Type;
  // Service definition created from protobufjs
  private firestoreService?: protobuf.Service;

  constructor(config: ServerConfig) {
    this.config = config;
    this.storage = new FirestoreStorage();
  }

  /**
   * Extract array_value from a protobufjs Message object
   */
  private extractArrayValueFromMessage(
    valueMessage: any,
    _fieldName: string,
  ): any {
    try {
      if (!this.ValueType || !this.protobufRoot) {
        return null;
      }

      // Get ArrayValue type for decoding
      const arrayValueType = this.protobufRoot.lookupType(
        'google.firestore.v1.ArrayValue',
      );
      if (!arrayValueType) {
        return null;
      }

      // Method 1: Try accessing via field descriptor
      const arrayValueField = this.ValueType.fields['array_value'];
      if (arrayValueField) {
        // Try direct access
        let arrayValue = valueMessage[arrayValueField.name];

        // Method 2: Try accessing via $oneof property
        if (!arrayValue && valueMessage.$oneof) {
          const oneofName = (this.ValueType as any).oneofsArray?.[0]?.name;
          if (oneofName && valueMessage.$oneof[oneofName] === 'array_value') {
            arrayValue = valueMessage.array_value;
          }
        }

        // Method 3: Try using field getter (if available)
        if (!arrayValue && (arrayValueField as any).get) {
          try {
            arrayValue = (arrayValueField as any).get(valueMessage);
          } catch {
            // Field getter failed
          }
        }

        // Method 4: Try toObject with oneofs: true
        if (!arrayValue) {
          try {
            const valueObj = this.ValueType.toObject(valueMessage, {
              longs: String,
              enums: String,
              bytes: String,
              defaults: false,
              arrays: true,
              objects: true,
              oneofs: true,
            });
            arrayValue = valueObj.array_value || valueObj.arrayValue;
          } catch {
            // toObject failed
          }
        }

        // Method 5: Try accessing internal $fields property
        if (!arrayValue && valueMessage.$fields) {
          const fields = valueMessage.$fields;
          if (fields && fields[arrayValueField.id]) {
            arrayValue = fields[arrayValueField.id];
          }
        }

        if (arrayValue) {
          // Convert ArrayValue to our format
          const arrayValueObj = arrayValueType.toObject(arrayValue, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: true,
            arrays: true,
            objects: true,
            oneofs: true,
          });

          return arrayValueObj;
        }
      }

      return null;
    } catch {
      return null;
    }
  }

  /**
   * Manually decode array_value from buffer
   */
  private manualDecodeArrayValue(
    buffer: Buffer,
    fieldName: string,
    _writeIndex: number,
  ): any {
    try {
      // Get ArrayValue type for decoding
      const arrayValueType = this.protobufRoot!.lookupType(
        'google.firestore.v1.ArrayValue',
      );
      if (!arrayValueType) {
        return null;
      }

      // Search for the field name in the buffer (as UTF-8 bytes)
      const fieldNameBytes = Buffer.from(fieldName, 'utf8');
      let searchStart = 0;

      // Look for the field name in the buffer
      // In protobuf, field names are encoded as strings
      while (searchStart < buffer.length - fieldNameBytes.length) {
        const index = buffer.indexOf(fieldNameBytes, searchStart);
        if (index === -1) {
          break;
        }

        // Check if this looks like a field name (preceded by field tag)
        // Field tag for map entry key (field 1) is typically 0x0A (wire type 2, field 1)
        // We're looking for the field name followed by array_value data

        // After the field name, we should find the array_value field
        // array_value is field 6 in Value message, wire type 2 (length-delimited)
        // Tag would be: (6 << 3) | 2 = 0x32
        const afterFieldName = index + fieldNameBytes.length;
        if (afterFieldName < buffer.length) {
          // Look for array_value tag (0x32) near the field name
          const searchWindow = buffer.slice(
            Math.max(0, index - 50),
            Math.min(buffer.length, index + fieldNameBytes.length + 200),
          );

          // Find array_value tag (0x32)
          const arrayValueTagIndex = searchWindow.indexOf(0x32);
          if (arrayValueTagIndex !== -1) {
            // Found potential array_value tag
            // The next byte(s) are the length of the ArrayValue message
            const tagPos = Math.max(0, index - 50) + arrayValueTagIndex + 1;
            if (tagPos < buffer.length) {
              // Decode varint length
              let length = 0;
              let shift = 0;
              let pos = tagPos;
              while (pos < buffer.length && shift < 32) {
                const byte = buffer[pos];
                length |= (byte & 0x7f) << shift;
                pos++;
                if ((byte & 0x80) === 0) {
                  break;
                }
                shift += 7;
              }

              // Extract the ArrayValue message
              if (pos + length <= buffer.length) {
                const arrayValueBuffer = buffer.slice(pos, pos + length);
                try {
                  const arrayValueMessage =
                    arrayValueType.decode(arrayValueBuffer);
                  const arrayValueObj = arrayValueType.toObject(
                    arrayValueMessage,
                    {
                      longs: String,
                      enums: String,
                      bytes: String,
                      defaults: true,
                      arrays: true,
                      objects: true,
                      oneofs: true,
                    },
                  );

                  return arrayValueObj;
                } catch {
                  // Decoding failed, continue searching
                }
              }
            }
          }
        }

        searchStart = index + 1;
      }

      return null;
    } catch {
      return null;
    }
  }

  /**
   * Create a gRPC service definition from protobufjs Service
   * This allows us to use protobufjs with @grpc/grpc-js
   */
  private createServiceDefinition(
    service: protobuf.Service,
  ): grpc.ServiceDefinition<any> {
    const serviceDefinition: Record<
      string,
      grpc.MethodDefinition<any, any>
    > = {};

    // For each method in the service, create a handler definition
    // The actual handlers will deserialize manually with protobufjs
    service.methodsArray.forEach((method) => {
      const methodName = method.name;
      // Get the actual Type objects from the root
      const requestTypeName = method.requestType;
      const responseTypeName = method.responseType;

      if (!this.protobufRoot) {
        throw new Error('protobufRoot not initialized');
      }

      const requestType = this.protobufRoot.lookupType(requestTypeName);
      const responseType = this.protobufRoot.lookupType(responseTypeName);

      if (!requestType || !responseType) {
        throw new Error(
          `Failed to find request or response type for ${methodName}`,
        );
      }

      // Create method definition
      // We'll use a custom serializer/deserializer that uses protobufjs
      // The path must match exactly what the client expects
      // service.fullName includes a leading dot, we need to remove it
      const serviceName = service.fullName.startsWith('.')
        ? service.fullName.substring(1)
        : service.fullName;
      const methodPath = `/${serviceName}/${methodName}`;
      this.logger.log('server', `Registering gRPC method: ${methodPath}`);
      serviceDefinition[methodName] = {
        path: methodPath,
        requestStream: method.requestStream || false,
        responseStream: method.responseStream || false,
        requestSerialize: (value: any): Buffer => {
          // Serialize using protobufjs
          const message = requestType.fromObject(value);
          return Buffer.from(requestType.encode(message).finish());
        },
        requestDeserialize: (buffer: Buffer): any => {
          // Deserialize using protobufjs
          const message = requestType.decode(buffer);

          // For Commit requests, extract oneof fields directly from protobufjs Message objects
          // before converting to plain object, as toObject() may lose oneof information
          if (methodName === 'Commit' && this.ValueType && this.protobufRoot) {
            const rawMessage = message as any;
            if (rawMessage.writes && Array.isArray(rawMessage.writes)) {
              rawMessage.writes.forEach((write: any) => {
                if (write && write.update && write.update.fields) {
                  const fieldsMessage = write.update.fields;
                  if (fieldsMessage && typeof fieldsMessage === 'object') {
                    const fieldNames = Object.keys(fieldsMessage).filter(
                      (key) =>
                        !key.startsWith('$') &&
                        key !== 'toJSON' &&
                        key !== 'toObject',
                    );

                    fieldNames.forEach((fieldName: string) => {
                      try {
                        const valueMessage = fieldsMessage[fieldName];
                        if (valueMessage && valueMessage.$type) {
                          const valueType = this.ValueType!;

                          // Try to access oneof fields via field descriptors
                          if (
                            valueType.oneofsArray &&
                            valueType.oneofsArray.length > 0
                          ) {
                            const oneof = valueType.oneofsArray[0];
                            if (oneof && oneof.name) {
                              const oneofCase = valueMessage[oneof.name];
                              if (
                                oneofCase === 'array_value' ||
                                oneofCase === 'arrayValue'
                              ) {
                                const arrayValueField =
                                  valueType.fields['array_value'];
                                if (arrayValueField) {
                                  try {
                                    const fieldId = arrayValueField.id;
                                    const arrayValue =
                                      valueMessage[fieldId] ||
                                      valueMessage['array_value'] ||
                                      valueMessage['arrayValue'];

                                    if (
                                      arrayValue &&
                                      !valueMessage._decodedArrayValue
                                    ) {
                                      valueMessage._decodedArrayValue =
                                        arrayValue;
                                    }
                                  } catch {
                                    // Field access failed, continue
                                  }
                                }
                              }
                            }
                          }

                          // Try accessing via $oneof property
                          if (valueMessage.$oneof) {
                            const oneofObj = valueMessage.$oneof;
                            Object.keys(oneofObj).forEach(
                              (oneofName: string) => {
                                if (
                                  oneofObj[oneofName] === 'array_value' ||
                                  oneofObj[oneofName] === 'arrayValue'
                                ) {
                                  const arrayValue =
                                    valueMessage.array_value ||
                                    valueMessage.arrayValue;
                                  if (
                                    arrayValue &&
                                    !valueMessage._decodedArrayValue
                                  ) {
                                    valueMessage._decodedArrayValue =
                                      arrayValue;
                                  }
                                }
                              },
                            );
                          }
                        }
                      } catch {
                        // Error accessing field, continue
                      }
                    });
                  }
                }
              });
            }
          }

          // Convert to plain object with all fields, including oneof fields
          const obj = requestType.toObject(message, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: false,
            arrays: true,
            objects: true,
            oneofs: true,
          });

          // For Commit requests, use @google-cloud/firestore's Serializer to decode arrays
          if (methodName === 'Commit' && obj.writes) {
            try {
              const {
                Serializer,
              } = require('@google-cloud/firestore/build/src/serializer');
              const {
                detectValueType,
              } = require('@google-cloud/firestore/build/src/convert');

              const mockFirestore = {
                _settings: { useBigInt: false },
                doc: (path: string) => ({ path }),
              };
              const serializer = new Serializer(mockFirestore as any);
              const rawMessage = message as any;

              obj.writes = obj.writes.map((write: any, index: number) => {
                if (write.update && write.update.fields) {
                  const fields: any = {};
                  const rawWrite = rawMessage.writes?.[index];

                  Object.keys(write.update.fields).forEach((key) => {
                    const fieldValue = write.update.fields[key];

                    try {
                      const valueType = detectValueType(fieldValue);

                      if (valueType === 'arrayValue') {
                        const rawField = rawWrite?.update?.fields?.[key];

                        if (rawField) {
                          const rawFieldObj = this.ValueType!.toObject(
                            rawField,
                            {
                              longs: String,
                              enums: String,
                              bytes: String,
                              defaults: true,
                              arrays: true,
                              objects: true,
                              oneofs: true,
                            },
                          );

                          try {
                            const decoded = serializer.decodeValue(rawFieldObj);
                            const { toFirestoreValue } = require('./utils');
                            fields[key] = {
                              arrayValue: {
                                values: decoded.map((v: any) =>
                                  toFirestoreValue(v),
                                ),
                              },
                            };
                          } catch {
                            fields[key] = fieldValue;
                          }
                        } else {
                          fields[key] = fieldValue;
                        }
                      } else {
                        fields[key] = fieldValue;
                      }
                    } catch {
                      fields[key] = fieldValue;
                    }
                  });

                  return {
                    ...write,
                    update: {
                      ...write.update,
                      fields,
                    },
                  };
                }
                return write;
              });
            } catch {
              // Serializer not available, continue with original data
            }
          }

          return obj;
        },
        responseSerialize: (value: any): Buffer => {
          // Serialize using protobufjs
          // When protobufjs loads from JSON proto, it uses jsonName which converts snake_case to camelCase
          // So fromObject() expects camelCase for fields that have jsonName defined
          // For BatchGetDocumentsResponse: read_time has jsonName "readTime", so we use camelCase
          // For Document: create_time has jsonName "createTime", update_time has jsonName "updateTime", so we use camelCase
          // For CommitResponse: write_results and commit_time do NOT have jsonName, so we use snake_case
          try {
            const message = responseType.fromObject(value);
            return Buffer.from(responseType.encode(message).finish());
          } catch (error) {
            this.logger.error(
              'grpc',
              `responseSerialize error for ${methodName}: ${error instanceof Error ? error.message : String(error)}`,
            );
            this.logger.error(
              'grpc',
              `responseSerialize value: ${JSON.stringify(value).substring(0, 500)}`,
            );
            throw error;
          }
        },
        responseDeserialize: (buffer: Buffer): any => {
          // Deserialize using protobufjs
          const message = responseType.decode(buffer);
          return responseType.toObject(message, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: true,
            arrays: true,
            objects: true,
            oneofs: true,
          });
        },
      };
    });

    return serviceDefinition as grpc.ServiceDefinition<any>;
  }

  public inferFieldType(fieldName: string): FieldType {
    return firestoreUtils.inferFieldType(fieldName);
  }

  public detectFieldType(value: FirestoreValue): FieldType | null {
    return firestoreUtils.detectFieldType(value);
  }

  public reconstructDocumentFields(
    document: FirestoreDocument,
  ): Record<string, FirestoreValue> {
    return firestoreUtils.reconstructDocumentFields(document, this.logger);
  }

  public parseDocumentPath(path: string): {
    projectId: string;
    databaseId: string;
    collectionId: string;
    docId: string;
  } | null {
    return firestoreUtils.parseDocumentPath(path);
  }

  public applyOrderBy(
    documents: FirestoreDocument[],
    orderBy: any[],
  ): FirestoreDocument[] {
    return firestoreUtils.applyOrderBy(documents, orderBy, (doc) =>
      firestoreUtils.reconstructDocumentFields(doc, this.logger),
    );
  }

  public applyQueryFilters(
    documents: FirestoreDocument[],
    filter: any,
  ): FirestoreDocument[] {
    return firestoreUtils.applyQueryFilters(documents, filter, this.logger);
  }

  public destroyStreamWithUnimplemented(
    call: grpc.ServerDuplexStream<any, any>,
    details: string,
  ): void {
    firestoreUtils.destroyStreamWithUnimplemented(call, details);
  }

  public emitUnimplementedWarning(rpcName: string): void {
    firestoreUtils.emitUnimplementedWarning(rpcName);
  }

  public async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Use an async IIFE to handle async operations
      (async () => {
        try {
          this.grpcServer = new grpc.Server();

          // Create service implementation (handlers are in firestore/handlers/*.ts)
          const serviceImplementation: any = {
            GetDocument: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleGetDocument(this, call, cb),

            ListDocuments: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleListDocuments(this, call, cb),

            RunQuery: (call: grpc.ServerWritableStream<any, any>) =>
              handleRunQuery(this, call),

            RunAggregationQuery: (call: grpc.ServerWritableStream<any, any>) =>
              handleRunAggregationQuery(this, call),

            CreateDocument: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleCreateDocument(this, call, cb),

            UpdateDocument: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleUpdateDocument(this, call, cb),

            DeleteDocument: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleDeleteDocument(this, call, cb),

            Commit: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleCommitWithProtobufjs(this, call, cb),

            BatchGetDocuments: (call: grpc.ServerWritableStream<any, any>) =>
              handleBatchGetDocuments(this, call),

            Listen: (call: grpc.ServerDuplexStream<any, any>) =>
              handleListen(this, call),

            Write: (call: grpc.ServerDuplexStream<any, any>) =>
              handleWrite(this, call),

            BatchWrite: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleUnimplementedUnary(this, call, cb, 'BatchWrite'),

            BeginTransaction: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleUnimplementedUnary(this, call, cb, 'BeginTransaction'),

            Rollback: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleUnimplementedUnary(this, call, cb, 'Rollback'),

            ListCollectionIds: (
              call: grpc.ServerUnaryCall<any, any>,
              cb: grpc.sendUnaryData<any>,
            ) => handleListCollectionIds(this, call, cb),
          };

          // Load proto: prefer local copy (proto/v1.json) so we always use the same
          // standard (camelCase from JSON, same as firebase-admin). No dependency on
          // node_modules resolution.
          const officialProtoPath = path.join(
            __dirname,
            '../../node_modules/@google-cloud/firestore/build/protos/google/firestore/v1/firestore.proto',
          );

          const localProtoPath = path.join(
            __dirname,
            '../proto/firestore.proto',
          );

          const protoPath = require('fs').existsSync(officialProtoPath)
            ? officialProtoPath
            : localProtoPath;

          // 1) Prefer local proto/v1.json (bundled copy, single standard = camelCase)
          const localJsonPath = path.join(__dirname, '../proto/v1.json');

          try {
            if (fs.existsSync(localJsonPath)) {
              this.logger.log(
                'server',
                'Loading proto from local proto/v1.json',
              );

              const jsonProto = JSON.parse(
                fs.readFileSync(localJsonPath, 'utf8'),
              );

              this.protobufRoot = protobuf.Root.fromJSON(jsonProto);
            } else {
              // 2) Fallback: try @google-cloud/firestore protos (when not bundled)
              let jsonProtoPath: string | null = null;

              try {
                jsonProtoPath =
                  require.resolve('@google-cloud/firestore/build/protos/v1.json');
              } catch {
                jsonProtoPath = path.join(
                  __dirname,
                  '../../node_modules/@google-cloud/firestore/build/protos/v1.json',
                );
              }

              if (fs.existsSync(jsonProtoPath)) {
                this.logger.log('server', 'Loading proto from JSON');

                const jsonProto = JSON.parse(
                  fs.readFileSync(jsonProtoPath, 'utf8'),
                );

                this.protobufRoot = protobuf.Root.fromJSON(jsonProto);
              } else if (protoPath === officialProtoPath) {
                this.logger.log(
                  'server',
                  'Loading proto from .proto file (fallback)',
                );

                this.protobufRoot = await protobuf.load(protoPath);
              } else {
                this.logger.log(
                  'server',
                  'Loading proto from local .proto file (fallback)',
                );

                const protoContent = fs.readFileSync(protoPath, 'utf8');
                this.protobufRoot = protobuf.parse(protoContent, {
                  keepCase: true,
                }).root;
              }
            }

            // Get message types for manual deserialization
            if (this.protobufRoot) {
              this.CommitRequestType = this.protobufRoot.lookupType(
                'google.firestore.v1.CommitRequest',
              );
              this.ValueType = this.protobufRoot.lookupType(
                'google.firestore.v1.Value',
              );

              if (!this.CommitRequestType || !this.ValueType) {
                throw new Error(
                  'Failed to find CommitRequest or Value message types in proto',
                );
              }

              // Get the Firestore service from protobufjs
              this.firestoreService = this.protobufRoot.lookupService(
                'google.firestore.v1.Firestore',
              );

              if (!this.firestoreService) {
                throw new Error(
                  'Failed to find Firestore service in proto definition',
                );
              }

              // Create a PackageDefinition manually from protobufjs
              // This is a workaround to use protobufjs with @grpc/grpc-js
              // We'll create handlers that deserialize manually with protobufjs
              const firestoreProto: any = {
                google: {
                  firestore: {
                    v1: {
                      Firestore: {
                        service: this.createServiceDefinition(
                          this.firestoreService,
                        ),
                      },
                    },
                  },
                },
              };

              // Add service to server using the protobufjs-based definition
              this.grpcServer.addService(
                firestoreProto.google.firestore.v1.Firestore.service,
                serviceImplementation,
              );

              this.logger.log('server', 'Loaded proto with protobufjs');
              this.logger.log(
                'server',
                `Registered methods: ${Object.keys(serviceImplementation).join(', ')}`,
              );
            }
          } catch (error) {
            reject(
              new Error(
                `Failed to load proto with protobufjs: ${error instanceof Error ? error.message : String(error)}`,
              ),
            );
            return;
          }

          // Bind server to address
          // Use IPv6 [::] to support both IPv4 and IPv6 connections
          const bindHost =
            this.config.host === 'localhost' ? '[::]' : this.config.host;
          const isIPv6 = bindHost === '[::]';
          const address = `${bindHost}:${this.config.port}`;
          this.grpcServer.bindAsync(
            address,
            grpc.ServerCredentials.createInsecure(),
            (error, port) => {
              if (error) {
                reject(error);
                return;
              }

              // DeprecationWarning: Calling start() is no longer necessary. It can be safely omitted.
              // this.grpcServer!.start();

              this.logger.log(
                'server',
                `Firestore gRPC emulator server running on ${this.config.host}:${port}${isIPv6 ? ' (IPv6 [::], accepts both IPv4 and IPv6 connections)' : ''}`,
              );
              this.logger.log(
                'server',
                `Project ID: ${this.config.projectId || 'default'}`,
              );

              resolve();
            },
          );
        } catch (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      })();
    });
  }

  public async stop(): Promise<void> {
    return new Promise((resolve) => {
      const promises: Promise<void>[] = [];

      if (this.grpcServer) {
        this.logger.log('grpc', 'Stopping server...');
        this.grpcServer.forceShutdown();
        this.grpcServer = undefined;
        this.logger.log('grpc', 'Firestore gRPC emulator server stopped');
      }

      Promise.all(promises).then(() => resolve());
      if (promises.length === 0) {
        resolve();
      }
    });
  }

  public getStorage(): FirestoreStorage {
    return this.storage;
  }

  public getConfig(): Readonly<ServerConfig> {
    return { ...this.config };
  }

  /**
   * Debug method to log all content in storage
   * Useful for debugging from external projects
   */
  public debugLog(): void {
    this.storage.debugLog();
  }
}
