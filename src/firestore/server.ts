/**
 * gRPC server that emulates Firestore API
 */

import * as fs from 'fs';
import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protobuf from 'protobufjs';
import { getLogger } from '../logger';
import { Storage } from './storage';
import {
  FirestoreDocument,
  FirestoreValue,
  ServerConfig,
  FieldType,
} from '../types';
import {
  toFirestoreDocument,
  buildDocumentPath,
  generateDocumentId,
  toTimestamp,
  toGrpcFields,
  sanitizeGrpcFieldsForResponse,
  normalizeGrpcValueToFirestoreValue,
} from '../utils';
import { config } from '../config';

export class FirestoreServer {
  private readonly storage: Storage;
  private readonly config: ServerConfig;
  private grpcServer?: grpc.Server;
  private readonly logger = getLogger();
  private protobufRoot?: protobuf.Root;
  // Message types for manual deserialization with protobufjs
  private CommitRequestType?: protobuf.Type;
  private ValueType?: protobuf.Type;
  // Service definition created from protobufjs
  private firestoreService?: protobuf.Service;

  constructor(config: ServerConfig) {
    this.config = config;
    this.storage = new Storage();
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

  /**
   * Infer field type from field name (heuristic for when proto-loader loses oneof data)
   */
  private inferFieldType(fieldName: string): FieldType {
    const lowerName = fieldName.toLowerCase();
    // Common array field names
    if (
      lowerName.includes('items') ||
      lowerName.includes('tags') ||
      lowerName.includes('list') ||
      lowerName.includes('array') ||
      lowerName.endsWith('s') // Plural nouns often indicate arrays
    ) {
      return 'arrayValue';
    }
    // Common map/object field names
    if (
      lowerName.includes('data') ||
      lowerName.includes('metadata') ||
      lowerName.includes('config') ||
      lowerName.includes('settings') ||
      lowerName.includes('options')
    ) {
      return 'mapValue';
    }
    // Default to arrayValue for empty objects (most common case)
    return 'arrayValue';
  }

  /**
   * Detect field type from normalized FirestoreValue
   */
  private detectFieldType(value: FirestoreValue): FieldType | null {
    if ('nullValue' in value) {
      return 'nullValue';
    }
    if ('booleanValue' in value) {
      return 'booleanValue';
    }
    if ('integerValue' in value) {
      return 'integerValue';
    }
    if ('doubleValue' in value) {
      return 'doubleValue';
    }
    if ('timestampValue' in value) {
      return 'timestampValue';
    }
    if ('stringValue' in value) {
      return 'stringValue';
    }
    if ('bytesValue' in value) {
      return 'bytesValue';
    }
    if ('referenceValue' in value) {
      return 'referenceValue';
    }
    if ('geoPointValue' in value) {
      return 'geoPointValue';
    }
    if ('arrayValue' in value) {
      return 'arrayValue';
    }
    if ('mapValue' in value) {
      return 'mapValue';
    }
    return null;
  }

  /**
   * Reconstruct document fields using stored metadata
   * This is used as a fallback to ensure field types are preserved
   * when reconstructing documents from storage
   */
  private reconstructDocumentFields(
    document: FirestoreDocument,
  ): Record<string, FirestoreValue> {
    if (!document.fieldTypes) {
      return document.fields ?? {};
    }

    const reconstructed: Record<string, FirestoreValue> = {
      ...(document.fields ?? {}),
    };

    Object.keys(document.fieldTypes).forEach((key) => {
      const expectedType = document.fieldTypes![key];
      const currentValue = reconstructed[key];

      // If field is missing or has wrong type, reconstruct it
      if (!currentValue || !(expectedType in currentValue)) {
        this.logger.log(
          'grpc',
          `Reconstructing field '${key}' with type '${expectedType}'`,
        );
        switch (expectedType) {
          case 'arrayValue':
            // For arrays, the actual values are lost during proto-loader deserialization
            // We can only reconstruct the type (empty array) - the actual values are not available
            // This is a known limitation: proto-loader loses oneof field data before it reaches our code
            reconstructed[key] = { arrayValue: { values: [] } };
            break;
          case 'mapValue':
            reconstructed[key] = { mapValue: { fields: {} } };
            break;
          case 'nullValue':
            reconstructed[key] = { nullValue: null };
            break;
          case 'booleanValue':
            reconstructed[key] = { booleanValue: false };
            break;
          case 'integerValue':
            reconstructed[key] = { integerValue: '0' };
            break;
          case 'doubleValue':
            reconstructed[key] = { doubleValue: 0 };
            break;
          case 'stringValue':
            reconstructed[key] = { stringValue: '' };
            break;
          default:
            // Keep existing value or use null
            reconstructed[key] = currentValue || { nullValue: null };
        }
      }
    });

    return reconstructed;
  }

  /**
   * Parse document path like "projects/{project}/databases/{db}/documents/{path...}/{doc}"
   * Supports both root collections (e.g. documents/users/doc1) and subcollections
   * (e.g. documents/events/ev1/users/user1). Returns collectionId as full path segment
   * (e.g. "users" or "events/ev1/users") and docId as the last segment.
   */
  private parseDocumentPath(path: string): {
    projectId: string;
    databaseId: string;
    collectionId: string;
    docId: string;
  } | null {
    const parts = path.split('/');
    const projectIndex = parts.indexOf('projects');
    const dbIndex = parts.indexOf('databases');
    const docsIndex = parts.indexOf('documents');

    if (
      projectIndex === -1 ||
      dbIndex === -1 ||
      docsIndex === -1 ||
      projectIndex + 1 >= parts.length ||
      dbIndex + 1 >= parts.length ||
      docsIndex + 1 >= parts.length
    ) {
      return null;
    }

    const projectId = parts[projectIndex + 1];
    const databaseId = parts[dbIndex + 1];
    const pathSegments = parts.slice(docsIndex + 1);
    if (pathSegments.length < 2) {
      return null;
    }
    const docId = pathSegments[pathSegments.length - 1];
    const collectionId = pathSegments.slice(0, -1).join('/');

    return { projectId, databaseId, collectionId, docId };
  }

  /**
   * Handle GetDocument gRPC call
   */
  private handleGetDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.name || '';

      this.logger.log('grpc', `[GetDocument] Called with path=${path}`);
      this.logger.log(
        'grpc',
        `[GetDocument] Request object: ${JSON.stringify(request).substring(0, 200)}`,
      );

      const parsed = this.parseDocumentPath(path);

      if (parsed) {
        this.logger.log(
          'grpc',
          `GetDocument DEBUG: Looking for document with projectId=${parsed.projectId}, databaseId=${parsed.databaseId}, collectionId=${parsed.collectionId}, docId=${parsed.docId}`,
        );
      }
      if (!parsed) {
        this.logger.log(
          'grpc',
          `GetDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      const document = this.storage.getDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      if (!document) {
        this.logger.log(
          'grpc',
          `GetDocument response: NOT_FOUND - Document not found`,
        );
        callback({
          code: grpc.status.NOT_FOUND,
          message: `Document not found: ${path}`,
        });
        return;
      }

      this.logger.log('grpc', `GetDocument response: SUCCESS - Document found`);
      // Reconstruct fields using metadata if needed
      const reconstructedFields = this.reconstructDocumentFields(document);
      // Convert to gRPC Document format with Timestamp
      // Ensure timestamps are always set (never null/undefined)
      const now = new Date();
      const defaultTimestamp = toTimestamp(now);
      const grpcDocument = {
        name: document.name,
        fields: toGrpcFields(reconstructedFields),
        createTime: document.createTime
          ? toTimestamp(new Date(document.createTime))
          : defaultTimestamp,
        updateTime: document.updateTime
          ? toTimestamp(new Date(document.updateTime))
          : defaultTimestamp,
      };
      callback(null, grpcDocument);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle ListDocuments gRPC call
   */
  private handleListDocuments(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const parent = request.parent || '';
      const collectionId = request.collectionId || '';

      this.logger.log(
        'grpc',
        `ListDocuments request: parent=${parent}, collectionId=${collectionId}`,
      );

      // Parse parent path like "projects/{project}/databases/{db}/documents"
      const parts = parent.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');
      const docsIndex = parts.indexOf('documents');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        docsIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `ListDocuments response: ERROR - Invalid parent path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid parent path: ${parent}`,
        });
        return;
      }

      const projectId = parts[projectIndex + 1];
      const databaseId = parts[dbIndex + 1];

      if (!collectionId) {
        this.logger.log(
          'grpc',
          `ListDocuments response: ERROR - collectionId required`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: 'collectionId is required',
        });
        return;
      }

      // Build full collection path: for subcollections parent is a document path
      // e.g. "projects/.../documents/events/ev123" + collectionId "users" -> "events/ev123/users"
      const pathAfterDocuments = parts.slice(docsIndex + 1).join('/');
      const collectionPath = pathAfterDocuments
        ? `${pathAfterDocuments}/${collectionId}`
        : collectionId;

      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionPath,
      );

      // Ensure each document has the correct resource name (full path including subcollection).
      // This way the client's doc.ref (built from document.name) points to the right collection,
      // and doc.ref.delete() sends DeleteDocument with the correct path (no confusion with root collections).
      for (const doc of documents) {
        const docId =
          (doc.name && doc.name.split('/').pop()) || (doc as any).id;
        if (docId) {
          doc.name = buildDocumentPath(
            projectId,
            databaseId,
            collectionPath,
            docId,
          );
        }
      }

      this.logger.log(
        'grpc',
        `ListDocuments response: SUCCESS - Found ${documents.length} documents`,
      );
      callback(null, {
        documents,
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle RunQuery gRPC call
   * This is the method used by collection.get() in Firebase Admin SDK
   * RunQuery is a server streaming RPC (client sends request, server streams responses)
   */
  private handleRunQuery(call: grpc.ServerWritableStream<any, any>): void {
    const rawRequest = call.request;
    setImmediate(() => {
    try {
      // Serialize to plain copy so we never touch proto getters again (they can block).
      let request: any;
      try {
        const seen = new WeakSet<object>();
        const json = JSON.stringify(rawRequest, (k, v) => {
          try {
            if (v !== null && typeof v === 'object') {
              if (seen.has(v)) return undefined;
              seen.add(v);
            }
            return v;
          } catch {
            return undefined;
          }
        });
        request = JSON.parse(json);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        call.destroy({
          code: grpc.status.INTERNAL,
          message: `RunQuery request serialization failed: ${msg}`,
        } as grpc.ServiceError);
        return;
      }
      const parent = request.parent || '';
      // Handle both camelCase (from JSON protos) and snake_case formats
      const structuredQuery =
        request.structured_query || request.structuredQuery || {};
      const from = structuredQuery.from;

      // Parse parent path: "projects/p/databases/db/documents" or "projects/p/databases/db/documents/events/ev123"
      const parts = parent.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');
      const docsIndex = parts.indexOf('documents');
      const projectId =
        projectIndex >= 0 && projectIndex + 1 < parts.length
          ? parts[projectIndex + 1]
          : 'test-project';
      const databaseId =
        dbIndex >= 0 && dbIndex + 1 < parts.length
          ? parts[dbIndex + 1]
          : '(default)';

      // Get collection ID from the query
      // According to proto, StructuredQuery.from is a single CollectionSelector, not an array
      // But with JSON protos, it might come as an object with numeric keys (array-like)
      let collectionId = '';
      if (from) {
        // Try collection_id first (standard field name)
        if (from.collection_id) {
          collectionId = from.collection_id;
        } else if (from.collectionId) {
          // Try camelCase (from JSON protos)
          collectionId = from.collectionId;
        } else if (typeof from === 'object') {
          // Handle array-like object (numeric keys)
          const keys = Object.keys(from);
          if (keys.length > 0) {
            const firstKey = keys[0];
            const firstFrom = from[firstKey];
            if (firstFrom) {
              collectionId =
                firstFrom.collection_id || firstFrom.collectionId || '';
            }
          }
        } else if (Array.isArray(from) && from.length > 0) {
          // Fallback: handle as array
          const firstFrom = from[0];
          if (firstFrom) {
            collectionId =
              firstFrom.collection_id || firstFrom.collectionId || '';
          }
        }
      }

      // Build full collection path for subcollections (same as ListDocuments)
      const pathAfterDocuments =
        docsIndex >= 0 ? parts.slice(docsIndex + 1).join('/') : '';
      const collectionPath = pathAfterDocuments
        ? `${pathAfterDocuments}/${collectionId}`
        : collectionId;

      // Get filter from query (where clause)
      const where =
        structuredQuery.where ||
        structuredQuery.filter ||
        structuredQuery.Where ||
        structuredQuery.Filter;

      if (where) {
        this.logger.log(
          'grpc',
          `RunQuery DEBUG: Query has filter/where clause: ${JSON.stringify(where).substring(0, 500)}`,
        );
      } else {
        this.logger.log(
          'grpc',
          `RunQuery DEBUG: Query has NO filter/where clause`,
        );
      }

      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Querying collection with projectId=${projectId}, databaseId=${databaseId}, collectionPath=${collectionPath}`,
      );

      // Get documents from storage
      let documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionPath,
      );

      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Found ${documents.length} documents in collection before filtering`,
      );

      // Apply filters if present
      if (where) {
        documents = this.applyQueryFilters(documents, where);
        this.logger.log(
          'grpc',
          `RunQuery DEBUG: Found ${documents.length} documents after filtering`,
        );
      }

      // Convert current time to Timestamp format (seconds and nanos)
      const now = new Date();
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      // Send responses as a stream. End the stream only after the last write
      // has been flushed, so the client receives all messages before stream close.
      // According to proto: RunQueryResponse has document = 1, read_time = 2, skipped_results = 3
      // When loaded from JSON, protobufjs uses camelCase: readTime, skippedResults
      const responses: any[] =
        documents.length === 0
          ? [
              {
                readTime: timestamp,
                skippedResults: 0,
              },
            ]
          : documents.map((doc) => {
              const docId = (doc.name && doc.name.split('/').pop()) || '';
              const documentPath = buildDocumentPath(
                projectId,
                databaseId,
                collectionPath,
                docId,
              );
              const reconstructedFields = this.reconstructDocumentFields(doc);
              const defaultTimestamp = toTimestamp(now);
              return {
                document: {
                  name: documentPath,
                  fields: toGrpcFields(reconstructedFields),
                  createTime: doc.createTime
                    ? toTimestamp(new Date(doc.createTime))
                    : defaultTimestamp,
                  updateTime: doc.updateTime
                    ? toTimestamp(new Date(doc.updateTime))
                    : defaultTimestamp,
                },
                readTime: timestamp,
                skippedResults: 0,
              };
            });

      // Send all responses then end the stream. Defer end() to the next tick so
      // the Writable has a chance to queue the writes; in gRPC/Node, calling
      // end() in the same tick as write() can leave the stream not fully flushed.
      for (const response of responses) {
        call.write(response);
      }
      call.end();
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      call.destroy({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      } as grpc.ServiceError);
    }
    });
  }

  /**
   * Handle RunAggregationQuery gRPC call (e.g. for count()).
   * Server streaming: one request, stream of RunAggregationQueryResponse.
   * Supports COUNT aggregation only; sum/avg return 0.
   */
  private handleRunAggregationQuery(
    call: grpc.ServerWritableStream<any, any>,
  ): void {
    const rawRequest = call.request;
    setImmediate(() => {
      try {
        let request: any;
        try {
          const seen = new WeakSet<object>();
          const json = JSON.stringify(rawRequest, (_, v) => {
            if (v !== null && typeof v === 'object') {
              if (seen.has(v)) return undefined;
              seen.add(v);
            }
            return v;
          });
          request = JSON.parse(json);
        } catch {
          call.destroy({
            code: grpc.status.INTERNAL,
            message: 'RunAggregationQuery request serialization failed',
          } as grpc.ServiceError);
          return;
        }
        const parent = request.parent || '';
        const structuredAggregationQuery =
          request.structured_aggregation_query ||
          request.structuredAggregationQuery ||
          {};
        const structuredQuery =
          structuredAggregationQuery.structured_query ||
          structuredAggregationQuery.structuredQuery ||
          {};
        const aggregations =
          structuredAggregationQuery.aggregations ||
          structuredAggregationQuery.Aggregations ||
          [];
        const from = structuredQuery.from;

        const parts = parent.split('/');
        const projectIndex = parts.indexOf('projects');
        const dbIndex = parts.indexOf('databases');
        const docsIndex = parts.indexOf('documents');
        const projectId =
          projectIndex >= 0 && projectIndex + 1 < parts.length
            ? parts[projectIndex + 1]
            : 'test-project';
        const databaseId =
          dbIndex >= 0 && dbIndex + 1 < parts.length
            ? parts[dbIndex + 1]
            : '(default)';

        let collectionId = '';
        if (from) {
          if (from.collection_id) collectionId = from.collection_id;
          else if (from.collectionId) collectionId = from.collectionId;
          else if (typeof from === 'object' && Object.keys(from).length > 0) {
            const first = from[Object.keys(from)[0]];
            collectionId = first?.collection_id || first?.collectionId || '';
          } else if (Array.isArray(from) && from.length > 0) {
            collectionId =
              from[0]?.collection_id || from[0]?.collectionId || '';
          }
        }

        const pathAfterDocuments =
          docsIndex >= 0 ? parts.slice(docsIndex + 1).join('/') : '';
        const collectionPath = pathAfterDocuments
          ? `${pathAfterDocuments}/${collectionId}`
          : collectionId;

        const where =
          structuredQuery.where ||
          structuredQuery.filter ||
          structuredQuery.Where ||
          structuredQuery.Filter;

        let documents = this.storage.listDocuments(
          projectId,
          databaseId,
          collectionPath,
        );
        if (where) {
          documents = this.applyQueryFilters(documents, where);
        }

        const count = documents.length;
        const now = new Date();
        const timestamp = {
          seconds: Math.floor(now.getTime() / 1000),
          nanos: (now.getTime() % 1000) * 1000000,
        };

        const aggregateFields: Record<string, { integerValue?: string; doubleValue?: number }> = {};
        for (const agg of Array.isArray(aggregations) ? aggregations : []) {
          const alias =
            agg.alias ?? agg.Alias ?? `alias_${Object.keys(aggregateFields).length}`;
          if (agg.count !== undefined || agg.Count !== undefined) {
            aggregateFields[alias] = { integerValue: String(count) };
          } else if (agg.sum !== undefined || agg.Sum !== undefined) {
            aggregateFields[alias] = { integerValue: '0' };
          } else if (agg.avg !== undefined || agg.Avg !== undefined) {
            aggregateFields[alias] = { doubleValue: 0 };
          }
        }
        if (Object.keys(aggregateFields).length === 0) {
          aggregateFields.count = { integerValue: String(count) };
        }

        const response = {
          result: { aggregateFields },
          readTime: timestamp,
        };
        call.write(response);
        call.end();
      } catch (error: unknown) {
        const msg =
          error instanceof Error ? error.message : String(error);
        call.destroy({
          code: grpc.status.INTERNAL,
          message: `RunAggregationQuery failed: ${msg}`,
        } as grpc.ServiceError);
      }
    });
  }

  /**
   * Apply query filters to documents
   */
  private applyQueryFilters(
    documents: FirestoreDocument[],
    filter: any,
  ): FirestoreDocument[] {
    if (!filter || !documents.length) {
      return documents;
    }

    // Handle different filter types
    // Filter can be: field_filter, composite_filter, or unary_filter
    const fieldFilter =
      filter.field_filter || filter.fieldFilter || filter.FieldFilter;
    const compositeFilter =
      filter.composite_filter ||
      filter.compositeFilter ||
      filter.CompositeFilter;
    const unaryFilter =
      filter.unary_filter || filter.unaryFilter || filter.UnaryFilter;

    if (fieldFilter) {
      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Applying field filter: ${JSON.stringify(fieldFilter).substring(0, 300)}`,
      );
      return this.applyFieldFilter(documents, fieldFilter);
    }

    if (compositeFilter) {
      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Applying composite filter: ${JSON.stringify(compositeFilter).substring(0, 500)}`,
      );
      const result = this.applyCompositeFilter(documents, compositeFilter);
      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Composite filter result: ${result.length} of ${documents.length} documents match`,
      );
      return result;
    }

    if (unaryFilter) {
      return this.applyUnaryFilter(documents, unaryFilter);
    }

    // If filter type is not recognized, return all documents
    return documents;
  }

  /**
   * Normalize filter value without invoking proto getters (which can block).
   * Builds a plain object from own property descriptors only, then normalizes.
   */
  private safeNormalizeFilterValue(value: any): FirestoreValue {
    if (value == null || typeof value !== 'object') {
      return { nullValue: null };
    }
    let keys: string[];
    try {
      keys = Object.getOwnPropertyNames(value);
    } catch {
      return { nullValue: null };
    }
    const plain: Record<string, unknown> = {};
    for (const key of keys) {
      try {
        const d = Object.getOwnPropertyDescriptor(value, key);
        if (d && 'value' in d) {
          plain[key] = d.value;
        }
      } catch {
        // skip key if descriptor access throws
      }
    }
    return normalizeGrpcValueToFirestoreValue(plain);
  }

  /**
   * Apply a field filter (e.g., field == value)
   */
  private applyFieldFilter(
    documents: FirestoreDocument[],
    fieldFilter: any,
  ): FirestoreDocument[] {
    const field =
      fieldFilter.field ||
      fieldFilter.Field ||
      fieldFilter.field_reference ||
      fieldFilter.fieldReference;
    const op =
      fieldFilter.op ||
      fieldFilter.Op ||
      fieldFilter.operator ||
      fieldFilter.Operator;
    const value =
      fieldFilter.value ||
      fieldFilter.Value ||
      fieldFilter.value_value ||
      fieldFilter.valueValue;

    if (!field || !op || !value) {
      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Field filter missing required fields: field=${!!field}, op=${!!op}, value=${!!value}`,
      );
      return documents;
    }

    // Get field path
    const fieldPath =
      field.field_path ||
      field.fieldPath ||
      field.field_path_string ||
      field.fieldPathString ||
      '';

    // Get operator
    const operator = op.toUpperCase();

    // Get filter value without invoking proto getters (which can block).
    let normalizedCompareValue: FirestoreValue;
    try {
      normalizedCompareValue = this.safeNormalizeFilterValue(value);
    } catch {
      normalizedCompareValue = { nullValue: null };
    }
    let filtered: FirestoreDocument[];
    try {
      filtered = documents.filter((doc) => {
        try {
          const docFields = this.reconstructDocumentFields(doc);
          let fieldValue = this.getFieldValueByPath(docFields, fieldPath);
          // Normalize document field value so timestamp objects become ISO strings
          if (fieldValue && typeof fieldValue === 'object' && Object.keys(fieldValue).length > 0) {
            fieldValue = normalizeGrpcValueToFirestoreValue(fieldValue);
          }
          return this.compareFieldValueWithNormalized(
            fieldValue,
            operator,
            normalizedCompareValue,
          );
        } catch {
          return false;
        }
      });
    } catch {
      filtered = [];
    }

    this.logger.log(
      'grpc',
      `RunQuery DEBUG: Field filter result: ${filtered.length} of ${documents.length} documents match`,
    );

    return filtered;
  }

  /**
   * Apply a composite filter (AND/OR of multiple filters)
   */
  private applyCompositeFilter(
    documents: FirestoreDocument[],
    compositeFilter: any,
  ): FirestoreDocument[] {
    const op =
      compositeFilter.op ||
      compositeFilter.Op ||
      compositeFilter.operator ||
      compositeFilter.Operator ||
      'AND';
    const filters =
      compositeFilter.filters ||
      compositeFilter.Filters ||
      compositeFilter.filter ||
      compositeFilter.Filter ||
      [];

    if (!Array.isArray(filters) || filters.length === 0) {
      return documents;
    }

    const operator = op.toUpperCase();

    if (operator === 'AND') {
      // All filters must match
      this.logger.log(
        'grpc',
        `RunQuery DEBUG: Applying AND composite filter with ${filters.length} sub-filters`,
      );
      let result = documents;
      for (let i = 0; i < filters.length; i++) {
        const filter = filters[i];
        this.logger.log(
          'grpc',
          `RunQuery DEBUG: Applying sub-filter ${i + 1}/${filters.length}: ${JSON.stringify(filter).substring(0, 200)}`,
        );
        const beforeCount = result.length;
        result = this.applyQueryFilters(result, filter);
        this.logger.log(
          'grpc',
          `RunQuery DEBUG: Sub-filter ${i + 1} result: ${result.length} of ${beforeCount} documents match`,
        );
      }
      return result;
    } else if (operator === 'OR') {
      // At least one filter must match
      const results: FirestoreDocument[] = [];
      for (const filter of filters) {
        const filtered = this.applyQueryFilters(documents, filter);
        for (const doc of filtered) {
          if (!results.find((d) => d.name === doc.name)) {
            results.push(doc);
          }
        }
      }
      return results;
    }

    return documents;
  }

  /**
   * Apply a unary filter (IS_NULL, IS_NAN, etc.)
   */
  private applyUnaryFilter(
    documents: FirestoreDocument[],
    unaryFilter: any,
  ): FirestoreDocument[] {
    const op =
      unaryFilter.op ||
      unaryFilter.Op ||
      unaryFilter.operator ||
      unaryFilter.Operator;
    const field =
      unaryFilter.field ||
      unaryFilter.Field ||
      unaryFilter.field_reference ||
      unaryFilter.fieldReference;

    if (!op || !field) {
      return documents;
    }

    const fieldPath =
      field.field_path ||
      field.fieldPath ||
      field.field_path_string ||
      field.fieldPathString ||
      '';
    const operator = op.toUpperCase();

    return documents.filter((doc) => {
      const docFields = this.reconstructDocumentFields(doc);
      const fieldValue = this.getFieldValueByPath(docFields, fieldPath);

      if (operator === 'IS_NULL') {
        return (
          !fieldValue ||
          fieldValue.nullValue !== undefined ||
          fieldValue === null
        );
      }
      if (operator === 'IS_NAN') {
        // For Firestore, IS_NAN only applies to numeric values
        if (!fieldValue || fieldValue.doubleValue === undefined) {
          return false;
        }
        const doubleVal =
          typeof fieldValue.doubleValue === 'string'
            ? parseFloat(fieldValue.doubleValue)
            : fieldValue.doubleValue;
        return typeof doubleVal === 'number' && isNaN(doubleVal);
      }
      if (operator === 'IS_NOT_NULL') {
        return (
          fieldValue &&
          fieldValue.nullValue === undefined &&
          fieldValue !== null
        );
      }

      return false;
    });
  }

  /**
   * Get field value by path (supports nested fields like "address.city")
   */
  private getFieldValueByPath(
    fields: Record<string, FirestoreValue>,
    path: string,
  ): FirestoreValue | undefined {
    if (!path) {
      return undefined;
    }
    const parts = path.split('.');
    let current: any = fields;

    for (const part of parts) {
      if (!current || typeof current !== 'object') {
        return undefined;
      }

      if (current[part]) {
        current = current[part];
      } else if (current.mapValue && current.mapValue.fields) {
        current = current.mapValue.fields[part];
      } else {
        return undefined;
      }
    }

    return current as FirestoreValue;
  }

  /**
   * Compare field value with operator (compareValue is raw gRPC value, will be normalized)
   */
  private compareFieldValue(
    fieldValue: FirestoreValue | undefined,
    operator: string,
    compareValue: any,
  ): boolean {
    const normalizedCompareValue =
      normalizeGrpcValueToFirestoreValue(compareValue);
    return this.compareFieldValueWithNormalized(
      fieldValue,
      operator,
      normalizedCompareValue,
    );
  }

  /**
   * Compare field value with operator (compareValue already normalized)
   * Missing field: only NOT_EQUAL matches (missing is not equal to any value).
   */
  private compareFieldValueWithNormalized(
    fieldValue: FirestoreValue | undefined,
    operator: string,
    normalizedCompareValue: FirestoreValue,
  ): boolean {
    if (!fieldValue) {
      // Missing field: in Firestore, "where('x','==',v)" does not match docs without x
      if (operator === 'EQUAL' || operator === '==') {
        return false;
      }
      if (operator === 'NOT_EQUAL' || operator === '!=') {
        return true;
      }
      return false;
    }

    switch (operator) {
      case 'EQUAL':
      case '==':
        return this.valuesEqual(fieldValue, normalizedCompareValue);
      case 'NOT_EQUAL':
      case '!=':
        return !this.valuesEqual(fieldValue, normalizedCompareValue);
      case 'LESS_THAN':
      case '<':
        return this.valueLessThan(fieldValue, normalizedCompareValue);
      case 'LESS_THAN_OR_EQUAL':
      case '<=':
        return (
          this.valueLessThan(fieldValue, normalizedCompareValue) ||
          this.valuesEqual(fieldValue, normalizedCompareValue)
        );
      case 'GREATER_THAN':
      case '>':
        return this.valueGreaterThan(fieldValue, normalizedCompareValue);
      case 'GREATER_THAN_OR_EQUAL':
      case '>=':
        return (
          this.valueGreaterThan(fieldValue, normalizedCompareValue) ||
          this.valuesEqual(fieldValue, normalizedCompareValue)
        );
      case 'ARRAY_CONTAINS':
        return this.arrayContains(fieldValue, normalizedCompareValue);
      case 'IN':
        return this.valueIn(fieldValue, normalizedCompareValue);
      case 'ARRAY_CONTAINS_ANY':
        return this.arrayContainsAny(fieldValue, normalizedCompareValue);
      default:
        return false;
    }
  }

  /**
   * Convert timestampValue (string ISO or object { seconds, nanos }) to milliseconds
   */
  private timestampToMs(v: FirestoreValue): number | null {
    const raw = (v as any).timestampValue ?? (v as any).timestamp_value;
    if (raw === undefined) return null;
    if (typeof raw === 'string') return new Date(raw).getTime();
    if (raw && typeof raw === 'object' && 'seconds' in raw) {
      const sec = Number((raw as any).seconds) || 0;
      const nan = Number((raw as any).nanos) || 0;
      return sec * 1000 + nan / 1000000;
    }
    return null;
  }

  /**
   * Check if two FirestoreValues are equal
   */
  private valuesEqual(a: FirestoreValue, b: FirestoreValue): boolean {
    // Compare by type
    if (a.stringValue !== undefined && b.stringValue !== undefined) {
      return a.stringValue === b.stringValue;
    }
    if (a.integerValue !== undefined && b.integerValue !== undefined) {
      return String(a.integerValue) === String(b.integerValue);
    }
    if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
      return Number(a.doubleValue) === Number(b.doubleValue);
    }
    if (a.booleanValue !== undefined && b.booleanValue !== undefined) {
      return a.booleanValue === b.booleanValue;
    }
    if (a.nullValue !== undefined && b.nullValue !== undefined) {
      return true;
    }
    const aTs = this.timestampToMs(a);
    const bTs = this.timestampToMs(b);
    if (aTs !== null && bTs !== null) return aTs === bTs;
    if (a.timestampValue !== undefined && b.timestampValue !== undefined) {
      return a.timestampValue === b.timestampValue;
    }
    return false;
  }

  /**
   * Check if value a is less than value b
   */
  private valueLessThan(a: FirestoreValue, b: FirestoreValue): boolean {
    if (a.stringValue !== undefined && b.stringValue !== undefined) {
      return a.stringValue < b.stringValue;
    }
    if (a.integerValue !== undefined && b.integerValue !== undefined) {
      return Number(a.integerValue) < Number(b.integerValue);
    }
    if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
      return Number(a.doubleValue) < Number(b.doubleValue);
    }
    const aTs = this.timestampToMs(a);
    const bTs = this.timestampToMs(b);
    if (aTs !== null && bTs !== null) return aTs < bTs;
    return false;
  }

  /**
   * Check if value a is greater than value b
   */
  private valueGreaterThan(a: FirestoreValue, b: FirestoreValue): boolean {
    if (a.stringValue !== undefined && b.stringValue !== undefined) {
      return a.stringValue > b.stringValue;
    }
    if (a.integerValue !== undefined && b.integerValue !== undefined) {
      return Number(a.integerValue) > Number(b.integerValue);
    }
    if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
      return Number(a.doubleValue) > Number(b.doubleValue);
    }
    const aTs = this.timestampToMs(a);
    const bTs = this.timestampToMs(b);
    if (aTs !== null && bTs !== null) return aTs > bTs;
    return false;
  }

  /**
   * Check if array contains value
   */
  private arrayContains(
    fieldValue: FirestoreValue,
    compareValue: FirestoreValue,
  ): boolean {
    if (!fieldValue.arrayValue || !fieldValue.arrayValue.values) {
      return false;
    }

    return fieldValue.arrayValue.values.some((val) =>
      this.valuesEqual(val, compareValue),
    );
  }

  /**
   * Check if value is in array
   */
  private valueIn(
    fieldValue: FirestoreValue,
    compareValue: FirestoreValue,
  ): boolean {
    if (!compareValue.arrayValue || !compareValue.arrayValue.values) {
      return false;
    }

    return compareValue.arrayValue.values.some((val) =>
      this.valuesEqual(fieldValue, val),
    );
  }

  /**
   * Check if array contains any of the values
   */
  private arrayContainsAny(
    fieldValue: FirestoreValue,
    compareValue: FirestoreValue,
  ): boolean {
    if (!fieldValue.arrayValue || !fieldValue.arrayValue.values) {
      return false;
    }
    if (!compareValue.arrayValue || !compareValue.arrayValue.values) {
      return false;
    }

    return fieldValue.arrayValue.values.some((val) =>
      compareValue.arrayValue!.values.some((compareVal) =>
        this.valuesEqual(val, compareVal),
      ),
    );
  }

  /**
   * Handle CreateDocument gRPC call
   */
  private handleCreateDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const parent = request.parent || '';
      const collectionId = request.collectionId || '';
      const docId = request.documentId || 'auto-generated';

      this.logger.log(
        'grpc',
        `CreateDocument request: parent=${parent}, collectionId=${collectionId}, documentId=${docId}`,
      );

      // Parse parent path (database or document path for subcollections)
      const parts = parent.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');
      const docsIndex = parts.indexOf('documents');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `CreateDocument response: ERROR - Invalid parent path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid parent path: ${parent}`,
        });
        return;
      }

      const projectId = parts[projectIndex + 1];
      const databaseId = parts[dbIndex + 1];
      const finalDocId = request.documentId || generateDocumentId();

      // Build full collection path for subcollections
      const pathAfterDocuments =
        docsIndex >= 0 ? parts.slice(docsIndex + 1).join('/') : '';
      const collectionPath = pathAfterDocuments
        ? `${pathAfterDocuments}/${collectionId}`
        : collectionId;

      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionPath,
        finalDocId,
      );

      // Convert request document to Firestore format; normalize gRPC values (e.g. timestamp object -> ISO string)
      const rawFields = request.document?.fields || {};
      const fields: Record<string, FirestoreValue> = {};
      Object.keys(rawFields).forEach((key) => {
        fields[key] = normalizeGrpcValueToFirestoreValue(rawFields[key]);
      });
      const document = toFirestoreDocument(documentPath, fields);
      document.name = documentPath;

      this.storage.setDocument(
        projectId,
        databaseId,
        collectionPath,
        finalDocId,
        document,
      );

      this.logger.log(
        'grpc',
        `CreateDocument response: SUCCESS - Created document at ${documentPath}`,
      );
      // Convert to gRPC Document format with Timestamp
      const now = new Date();
      const defaultTimestamp = toTimestamp(now);
      const grpcDocument = {
        name: document.name,
        fields: toGrpcFields(document.fields),
        createTime: document.createTime
          ? toTimestamp(new Date(document.createTime))
          : defaultTimestamp,
        updateTime: document.updateTime
          ? toTimestamp(new Date(document.updateTime))
          : defaultTimestamp,
      };
      callback(null, grpcDocument);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle UpdateDocument gRPC call
   */
  private handleUpdateDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.document?.name || '';

      this.logger.log('grpc', `UpdateDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
      if (!parsed) {
        this.logger.log(
          'grpc',
          `UpdateDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      // Get existing document or create new one
      const existingDoc = this.storage.getDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      const rawFields = request.document?.fields || {};
      const normalizedFields: Record<string, FirestoreValue> = {};
      Object.keys(rawFields).forEach((key) => {
        normalizedFields[key] = normalizeGrpcValueToFirestoreValue(rawFields[key]);
      });
      const document = toFirestoreDocument(path, normalizedFields);
      document.name = path;

      if (existingDoc) {
        document.createTime = existingDoc.createTime;
      }
      document.updateTime = new Date().toISOString();

      this.storage.setDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
        document,
      );

      this.logger.log(
        'grpc',
        `UpdateDocument response: SUCCESS - ${existingDoc ? 'Updated' : 'Created'} document at ${path}`,
      );
      // Convert to gRPC Document format with Timestamp
      const grpcDocument = {
        name: document.name,
        fields: toGrpcFields(document.fields),
        createTime: document.createTime
          ? toTimestamp(new Date(document.createTime))
          : toTimestamp(new Date()),
        updateTime: document.updateTime
          ? toTimestamp(new Date(document.updateTime))
          : toTimestamp(new Date()),
      };
      callback(null, grpcDocument);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle Commit gRPC call with protobufjs deserialization
   * With protobufjs, messages are now correctly deserialized with oneof fields preserved
   */
  private handleCommitWithProtobufjs(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    // With protobufjs, call.request is now correctly deserialized with oneof fields
    // including arrays! We can use the standard handler
    this.handleCommit(call, callback);
  }

  /**
   * Handle Commit gRPC call
   * This is used by Firebase Admin SDK for write operations (set, add, update, delete)
   */
  private handleCommit(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    let callbackInvoked = false;
    const safeCallback: grpc.sendUnaryData<any> = (err, value) => {
      if (callbackInvoked) return;
      callbackInvoked = true;
      callback(err, value);
    };

    try {
      this.logger.log('grpc', '[Commit] Called');
      const request = call.request;
      const database = request.database || '';
      const rawWrites = request.writes;
      const writes = Array.isArray(rawWrites)
        ? rawWrites
        : rawWrites && typeof rawWrites === 'object'
          ? Object.values(rawWrites)
          : [];

      this.logger.log(
        'grpc',
        `[Commit] Request: database=${database}, writes count=${writes.length}`,
      );

      // Parse database path like "projects/{project}/databases/{db}"
      const parts = database.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `Commit: ${writes.length} writes - ERROR: Invalid database path`,
        );
        safeCallback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid database path: ${database}`,
        }, null);
        return;
      }

      // Group writes by collection and operation type for compact logging
      const writesByCollection = new Map<
        string,
        { updates: string[]; deletes: string[] }
      >();
      for (const write of writes) {
        let docPath = '';
        let operation: 'update' | 'delete' = 'update';
        if (write.update) {
          docPath = write.update.name || '';
          operation = 'update';
        } else {
          const deletePath = write.delete ?? write['delete'];
          if (deletePath != null) {
            docPath = typeof deletePath === 'string' ? deletePath : String(deletePath);
            operation = 'delete';
          }
        }

        if (docPath) {
          const parsed = this.parseDocumentPath(docPath);
          if (parsed) {
            const key = `${parsed.collectionId}`;
            if (!writesByCollection.has(key)) {
              writesByCollection.set(key, { updates: [], deletes: [] });
            }
            const collectionWrites = writesByCollection.get(key)!;
            if (operation === 'update') {
              collectionWrites.updates.push(parsed.docId);
            } else {
              collectionWrites.deletes.push(parsed.docId);
            }
          }
        }
      }

      // Create compact log format: collection[+doc1,doc2 -doc3] collection2[+doc4]
      const logParts: string[] = [];
      for (const [collection, ops] of writesByCollection.entries()) {
        const parts: string[] = [];
        if (ops.updates.length > 0) {
          parts.push(`+${ops.updates.join(',')}`);
        }
        if (ops.deletes.length > 0) {
          parts.push(`-${ops.deletes.join(',')}`);
        }
        if (parts.length > 0) {
          logParts.push(`${collection}[${parts.join(' ')}]`);
        }
      }
      const compactLog = logParts.join(' ');

      this.logger.log(
        'grpc',
        `Commit: ${compactLog || `${writes.length} writes`}`,
      );

      const writeResults: any[] = [];
      const now = new Date();
      // Use the same timestamp format as RunQuery (inline format)
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      // Process each write
      for (const write of writes) {
        if (write.update) {
          // Update or create document
          // The document already comes in Firestore format from the client
          const doc = write.update;
          const docPath = doc.name || '';

          const parsed = this.parseDocumentPath(docPath);

          if (!parsed) {
            this.logger.log(
              'grpc',
              `Commit response: ERROR - Invalid document path in write`,
            );
            safeCallback({
              code: grpc.status.INVALID_ARGUMENT,
              message: `Invalid document path: ${docPath}`,
            }, null);
            return;
          }

          // Check if document exists
          const existingDoc = this.storage.getDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
          );

          // Convert Firestore Document to our internal format
          // doc.fields may be a gRPC Map or plain object
          const fields: Record<string, FirestoreValue> = {};
          const fieldTypes: Record<string, FieldType> = {};

          let fieldsSource: Record<string, any> = doc.fields || {};
          if (fieldsSource && typeof (fieldsSource as any).toObject === 'function') {
            try {
              fieldsSource = (fieldsSource as any).toObject({ longs: String, enums: String, bytes: String, defaults: true, oneofs: true });
            } catch {
              fieldsSource = doc.fields || {};
            }
          }
          if (fieldsSource && typeof fieldsSource === 'object') {
            Object.keys(fieldsSource).forEach((key) => {
              let value = fieldsSource[key];
              // Expand protobuf Message to plain object so oneof (e.g. timestamp_value) is readable
              if (value && typeof value === 'object' && typeof (value as any).toObject === 'function') {
                try {
                  value = (value as any).toObject({ longs: String, enums: String, bytes: String, defaults: true, oneofs: true });
                } catch {
                  // keep value as-is
                }
              }
              // Ensure the value is a proper FirestoreValue object
              if (value && typeof value === 'object') {
                // Always try to normalize first (handles gRPC Value with timestamp_value etc.)
                const normalizedValue = normalizeGrpcValueToFirestoreValue(value);
                if (normalizedValue && Object.keys(normalizedValue).length > 0) {
                  fields[key] = normalizedValue;
                  const detectedType = this.detectFieldType(normalizedValue);
                  if (detectedType) fieldTypes[key] = detectedType;
                  return;
                }
                if (Object.keys(value).length === 0) {
                  // Try to use protobufjs to deserialize the field manually
                  // We need to re-serialize the write.update document and deserialize it with protobufjs
                  if (this.protobufRoot && write.update) {
                    try {
                      // Get the Value message type from protobufjs
                      const ValueType = this.protobufRoot.lookupType(
                        'google.firestore.v1.Value',
                      );
                      if (ValueType) {
                        // Try to serialize the document field and deserialize it with protobufjs
                        // First, we need to get the raw field data from the write
                        const rawField = write.update.fields?.[key];
                        if (rawField) {
                          // Try to serialize the field using proto-loader's serialization
                          // and then deserialize it with protobufjs
                          // But we don't have the raw buffer, so we need another approach

                          // Alternative: try to access the field using protobufjs's message structure
                          // The field should be a Value message with array_value set
                          // Let's try to reconstruct it from the write object
                          this.logger.log(
                            'grpc',
                            `Commit: Attempting protobufjs deserialization for empty field '${key}'`,
                          );

                          // Since we don't have the raw buffer, we'll try to access
                          // the field through the protobufjs message structure if available
                          // For now, fall back to inference
                        }
                      }
                    } catch (error) {
                      this.logger.log(
                        'grpc',
                        `Commit: protobufjs deserialization failed for '${key}': ${error instanceof Error ? error.message : String(error)}`,
                      );
                    }
                  }

                  // Fallback: infer field type from context
                  const inferredType: FieldType = this.inferFieldType(key);
                  // Store metadata about the expected type
                  fieldTypes[key] = inferredType;
                  // Store as empty value of the inferred type
                  if (inferredType === 'arrayValue') {
                    fields[key] = { arrayValue: { values: [] } };
                  } else if (inferredType === 'mapValue') {
                    fields[key] = { mapValue: { fields: {} } };
                  } else {
                    // For other types, store as null for now
                    fields[key] = { nullValue: null };
                  }
                  return;
                }
                // Use recursive function to normalize gRPC value to FirestoreValue
                // This handles all value types including nested arrays and maps
                const normalizedValueFallback =
                  normalizeGrpcValueToFirestoreValue(value);
                // Only add field if normalizedValueFallback has at least one property
                if (Object.keys(normalizedValueFallback).length > 0) {
                  fields[key] = normalizedValueFallback;
                  // Store the detected type in metadata
                  const detectedType = this.detectFieldType(normalizedValueFallback);
                  if (detectedType) {
                    fieldTypes[key] = detectedType;
                  }
                }
              }
            });
          }

          // Apply updateTransforms (e.g. serverTimestamp) to fields before merging
          const updateTransforms =
            (write as any).updateTransforms || (write as any).update_transforms;
          if (Array.isArray(updateTransforms)) {
            updateTransforms.forEach((t: any) => {
              if (!t) return;
              const fieldPath =
                t.fieldPath || t.field_path || t.field || t.Field || '';
              if (!fieldPath) return;

              // Handle setToServerValue: REQUEST_TIME (serverTimestamp)
              const serverValue =
                t.setToServerValue || t.set_to_server_value || t.serverValue;
              if (
                serverValue === 'REQUEST_TIME' ||
                serverValue === 1 || // enum value for REQUEST_TIME
                serverValue === 'REQUEST_TIME_UNSPECIFIED'
              ) {
                const iso = now.toISOString();
                fields[fieldPath] = { timestampValue: iso };
                const detectedType = this.detectFieldType(fields[fieldPath]);
                if (detectedType) {
                  fieldTypes[fieldPath] = detectedType;
                }
              }
            });
          }

          // Apply top-level transform with fieldTransforms (less common, but supported)
          const transform = (write as any).transform;
          const fieldTransforms =
            transform &&
            (transform.fieldTransforms || transform.field_transforms);
          if (Array.isArray(fieldTransforms)) {
            fieldTransforms.forEach((t: any) => {
              if (!t) return;
              const fieldPath =
                t.fieldPath || t.field_path || t.field || t.Field || '';
              if (!fieldPath) return;

              const serverValue =
                t.setToServerValue || t.set_to_server_value || t.serverValue;
              if (
                serverValue === 'REQUEST_TIME' ||
                serverValue === 1 ||
                serverValue === 'REQUEST_TIME_UNSPECIFIED'
              ) {
                const iso = now.toISOString();
                fields[fieldPath] = { timestampValue: iso };
                const detectedType = this.detectFieldType(fields[fieldPath]);
                if (detectedType) {
                  fieldTypes[fieldPath] = detectedType;
                }
              }
            });
          }

          // For updates, merge with existing fields
          let finalFields = fields;
          if (existingDoc && write.updateMask) {
            // If updateMask is present, merge with existing fields
            const existingFields = existingDoc.fields || {};
            finalFields = { ...existingFields, ...fields };
          } else if (existingDoc && !write.updateMask) {
            // If no updateMask, merge with existing fields (update operation)
            const existingFields = existingDoc.fields || {};
            finalFields = { ...existingFields, ...fields };
          }

          const document: FirestoreDocument = {
            name: docPath,
            fields: finalFields,
            createTime: existingDoc?.createTime || new Date().toISOString(),
            updateTime: new Date().toISOString(),
            fieldTypes:
              Object.keys(fieldTypes).length > 0 ? fieldTypes : undefined,
          };

          // Debug logging to understand document storage
          this.logger.log(
            'grpc',
            `Commit DEBUG: Saving document with projectId=${parsed.projectId}, databaseId=${parsed.databaseId}, collectionId=${parsed.collectionId}, docId=${parsed.docId}`,
          );

          this.storage.setDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
            document,
          );

          // Verify document was saved correctly
          const savedDoc = this.storage.getDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
          );
          if (savedDoc) {
            this.logger.log(
              'grpc',
              `Commit DEBUG: Document verified after save - exists in storage`,
            );
          } else {
            this.logger.log(
              'grpc',
              `Commit DEBUG: WARNING - Document NOT found in storage after save!`,
            );
          }

          // According to proto: WriteResult has update_time = 1
          // When using protobufjs with JSON proto, fields should be in snake_case
          writeResults.push({
            update_time: timestamp, // Use snake_case for JSON proto
          });
        } else {
          const deletePath = write.delete ?? write['delete'];
          if (deletePath != null) {
            // Delete document
            const docPath = typeof deletePath === 'string' ? deletePath : String(deletePath);
            const parsed = this.parseDocumentPath(docPath);

            if (!parsed) {
              this.logger.log(
                'grpc',
                `Commit response: ERROR - Invalid document path in delete`,
              );
              safeCallback({
                code: grpc.status.INVALID_ARGUMENT,
                message: `Invalid document path: ${docPath}`,
              }, null);
              return;
            }

            this.storage.deleteDocument(
              parsed.projectId,
              parsed.databaseId,
              parsed.collectionId,
              parsed.docId,
            );

            // According to proto: WriteResult has update_time = 1
            // When using protobufjs with JSON proto, fields should be in snake_case
            writeResults.push({
              update_time: timestamp, // Use snake_case for JSON proto
            });
          }
        }
      }

      // Log response with same compact format
      this.logger.log(
        'grpc',
        `Commit: ${compactLog || `${writes.length} writes`} ✓`,
      );

      // According to proto: CommitResponse has write_results = 1, commit_time = 2
      // Field order matters in protobuf, so write_results must come first
      // When using protobufjs with JSON proto, fields should be in snake_case
      safeCallback(null, {
        write_results: writeResults,
        commit_time: timestamp, // Use snake_case for JSON proto
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      this.logger.error('grpc', `Commit error: ${errorMessage}`);
      safeCallback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      }, null);
    }
  }

  /**
   * Handle BatchGetDocuments gRPC call
   * This is used by Firebase Admin SDK to get multiple documents efficiently
   */
  private handleBatchGetDocuments(
    call: grpc.ServerWritableStream<any, any>,
  ): void {
    try {
      const request = call.request;
      const database = request.database || '';
      const documents = request.documents || [];

      // Parse database path like "projects/{project}/databases/{db}"
      const parts = database.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `BatchGetDocuments request: database=${database}, documents=${documents.length}`,
        );
        this.logger.log(
          'grpc',
          `BatchGetDocuments response: ERROR - Invalid database path`,
        );
        const error: grpc.ServiceError = {
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid database path: ${database}`,
          name: 'InvalidArgument',
          details: `Invalid database path: ${database}`,
          metadata: new grpc.Metadata(),
        };
        call.destroy(error);
        return;
      }

      // Group documents by collection for compact logging
      const docsByCollection = new Map<string, string[]>();
      for (const docPath of documents) {
        const parsed = this.parseDocumentPath(docPath);
        if (parsed) {
          const key = `${parsed.collectionId}`;
          if (!docsByCollection.has(key)) {
            docsByCollection.set(key, []);
          }
          docsByCollection.get(key)!.push(parsed.docId);
        }
      }

      // Create compact log format: collection/doc1,doc2 collection2/doc3
      const logParts: string[] = [];
      for (const [collection, docIds] of docsByCollection.entries()) {
        logParts.push(`${collection}/${docIds.join(',')}`);
      }
      const compactLog = logParts.join(' ');

      this.logger.log(
        'grpc',
        `BatchGetDocuments: ${compactLog || `${documents.length} docs`}`,
      );

      // Build all responses first, then write in sequence so each write completes before we end the stream.
      const responses: any[] = [];
      for (const docPath of documents) {
        const parsed = this.parseDocumentPath(docPath);

        if (!parsed) {
          this.logger.log(
            'grpc',
            `BatchGetDocuments response: MISSING - Invalid document path: ${docPath}`,
          );
          const now = new Date();
          responses.push({
            result: 'missing',
            missing: docPath,
            readTime: toTimestamp(now),
          });
          continue;
        }

        const document = this.storage.getDocument(
          parsed.projectId,
          parsed.databaseId,
          parsed.collectionId,
          parsed.docId,
        );

        if (document) {
          const reconstructedFields = this.reconstructDocumentFields(document);
          const now = new Date();
          const defaultTimestamp = toTimestamp(now);
          const grpcFields = sanitizeGrpcFieldsForResponse(
            toGrpcFields(reconstructedFields),
          );
          const grpcDocument = {
            name: document.name,
            fields: grpcFields,
            createTime: document.createTime
              ? toTimestamp(new Date(document.createTime))
              : defaultTimestamp,
            updateTime: document.updateTime
              ? toTimestamp(new Date(document.updateTime))
              : defaultTimestamp,
          };
          responses.push({
            result: 'found',
            found: grpcDocument,
            readTime: defaultTimestamp,
          });
        } else {
          const now = new Date();
          responses.push({
            result: 'missing',
            missing: docPath,
            readTime: toTimestamp(now),
          });
        }
      }

      // Write each response and only end after the last write has been flushed (so client receives all messages).
      const writeNext = (index: number) => {
        if (index >= responses.length) {
          this.logger.log(
            'grpc',
            `BatchGetDocuments: ${compactLog || `${documents.length} docs`} ✓`,
          );
          call.end();
          return;
        }
        const res = responses[index];
        const outcome = res.found ? 'FOUND' : 'MISSING';
        const path = res.found ? res.found.name : res.missing;
        this.logger.log(
          'grpc',
          `BatchGetDocuments response[${index}]: ${outcome} ${path}`,
        );
        call.write(responses[index], (err?: Error) => {
          if (err) {
            this.logger.error(
              'grpc',
              `BatchGetDocuments write error: ${err.message}`,
            );
            call.destroy(err);
            return;
          }
          writeNext(index + 1);
        });
      };
      writeNext(0);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      this.logger.error('grpc', `BatchGetDocuments error: ${errorMessage}`);
      const serviceError: grpc.ServiceError = {
        code: grpc.status.INTERNAL,
        message: errorMessage,
        name: 'InternalError',
        details: errorMessage,
        metadata: new grpc.Metadata(),
      };
      call.destroy(serviceError);
    }
  }

  /**
   * Handle DeleteDocument gRPC call
   */
  private handleDeleteDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.name || '';

      this.logger.log('grpc', `DeleteDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
      if (!parsed) {
        this.logger.log(
          'grpc',
          `DeleteDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      const deleted = this.storage.deleteDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      if (!deleted) {
        this.logger.log(
          'grpc',
          `DeleteDocument response: NOT_FOUND - Document not found`,
        );
        callback({
          code: grpc.status.NOT_FOUND,
          message: `Document not found: ${path}`,
        });
        return;
      }

      this.logger.log(
        'grpc',
        `DeleteDocument response: SUCCESS - Document deleted`,
      );
      callback(null, {});
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Stub for Listen RPC (real-time listeners).
   * Not implemented by the emulator; immediately ends the stream so the client
   * does not hang waiting for responses. Without this, a Listen call would have
   * no handler and could block the test process.
   */
  private handleListen(call: grpc.ServerDuplexStream<any, any>): void {
    this.logger.log('grpc', 'Listen RPC called (not implemented, closing stream)');
    this.destroyStreamWithUnimplemented(
      call,
      'Listen (real-time) is not supported by this emulator',
    );
  }

  /**
   * Stub for Write RPC (streaming writes). Same as Listen: bidi stream, must be
   * closed immediately or the client can hang during cleanup/transactions.
   */
  private handleWrite(call: grpc.ServerDuplexStream<any, any>): void {
    this.logger.log('grpc', 'Write RPC called (not implemented, closing stream)');
    this.destroyStreamWithUnimplemented(
      call,
      'Write (streaming) is not supported by this emulator',
    );
  }

  private destroyStreamWithUnimplemented(
    call: grpc.ServerDuplexStream<any, any>,
    details: string,
  ): void {
    call.destroy(
      Object.assign(new Error(details), {
        code: grpc.status.UNIMPLEMENTED,
        details,
      }) as grpc.ServiceError,
    );
  }

  /**
   * Emit a visible warning (or throw) when an unimplemented RPC is called.
   * Config: logs.onUnimplemented = 'warn' (default) | 'throw'
   */
  private emitUnimplementedWarning(rpcName: string): void {
    const mode =
      config.getString('logs.onUnimplemented', 'warn') === 'throw'
        ? 'throw'
        : 'warn';
    const msg = `[FIREBASE-MOCKER] RPC NOT IMPLEMENTED: ${rpcName}. This emulator does not support this operation.`;
    const hint = `Set firebaseMocker.addConfig({ logs: { onUnimplemented: 'warn' } }) to only log, or 'throw' to fail the process.`;
    if (mode === 'throw') {
      throw new Error(`${msg} ${hint}`);
    }
    process.stderr.write(
      `\n*** ${msg} ***\n${hint}\n\n`,
      () => {},
    );
  }

  /**
   * Stub for BatchWrite (and other unary RPCs not implemented). Emits a
   * visible warning (or throws if logs.onUnimplemented === 'throw'), then
   * responds with UNIMPLEMENTED so the client does not hang.
   */
  private handleUnimplementedUnary(
    _call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
    rpcName: string,
  ): void {
    try {
      this.emitUnimplementedWarning(rpcName);
    } catch (err) {
      callback(err as grpc.ServiceError, null);
      return;
    }
    callback(
      {
        code: grpc.status.UNIMPLEMENTED,
        message: `${rpcName} is not supported by this emulator`,
        details: `${rpcName} is not supported by this emulator`,
      } as grpc.ServiceError,
      null,
    );
  }

  public async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Use an async IIFE to handle async operations
      (async () => {
        try {
          this.grpcServer = new grpc.Server();

          // Create service implementation
          // For Commit, we'll use a wrapper that deserializes with protobufjs
          const serviceImplementation: any = {
            GetDocument: this.handleGetDocument.bind(this),
            ListDocuments: this.handleListDocuments.bind(this),
            RunQuery: this.handleRunQuery.bind(this),
            RunAggregationQuery: this.handleRunAggregationQuery.bind(this),
            CreateDocument: this.handleCreateDocument.bind(this),
            UpdateDocument: this.handleUpdateDocument.bind(this),
            DeleteDocument: this.handleDeleteDocument.bind(this),
            Commit: this.handleCommitWithProtobufjs.bind(this),
            BatchGetDocuments: this.handleBatchGetDocuments.bind(this),
            Listen: this.handleListen.bind(this),
            Write: this.handleWrite.bind(this),
            BatchWrite: (call: grpc.ServerUnaryCall<any, any>, cb: grpc.sendUnaryData<any>) =>
              this.handleUnimplementedUnary(call, cb, 'BatchWrite'),
            BeginTransaction: (call: grpc.ServerUnaryCall<any, any>, cb: grpc.sendUnaryData<any>) =>
              this.handleUnimplementedUnary(call, cb, 'BeginTransaction'),
            Rollback: (call: grpc.ServerUnaryCall<any, any>, cb: grpc.sendUnaryData<any>) =>
              this.handleUnimplementedUnary(call, cb, 'Rollback'),
            ListCollectionIds: (call: grpc.ServerUnaryCall<any, any>, cb: grpc.sendUnaryData<any>) =>
              this.handleUnimplementedUnary(call, cb, 'ListCollectionIds'),
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
                this.logger.log(
                  'server',
                  'Loading proto from JSON (same method as firebase-admin)',
                );
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
        this.logger.log('server', 'Stopping server...');
        this.grpcServer.forceShutdown();
        this.grpcServer = undefined;
        this.logger.log('server', 'Firestore gRPC emulator server stopped');
      }

      Promise.all(promises).then(() => resolve());
      if (promises.length === 0) {
        resolve();
      }
    });
  }

  public getStorage(): Storage {
    return this.storage;
  }

  public getConfig(): Readonly<ServerConfig> {
    return { ...this.config };
  }

  /**
   * Debug method to log all content in storage
   * Useful for debugging from external projects
   */
  public debugLogStorage(): void {
    this.storage.debugLog();
  }

  /**
   * Alias for debugLogStorage() for consistency with Auth server API
   */
  public debugLog(): void {
    this.debugLogStorage();
  }
}
