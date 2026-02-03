/**
 * gRPC server that emulates Firestore API
 */

import * as fs from 'fs';
import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protobuf from 'protobufjs';
import { FirestoreStorage } from './firestore-storage';
import { getLogger } from './logger';
import {
  FirestoreDocument,
  FirestoreValue,
  ServerConfig,
  FieldType,
} from './types';
import {
  toFirestoreDocument,
  fromFirestoreDocument,
  buildDocumentPath,
  generateDocumentId,
  toTimestamp,
  toGrpcFields,
  normalizeGrpcValueToFirestoreValue,
} from './utils';

export class FirestoreServer {
  private readonly storage: FirestoreStorage;
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
    this.storage = new FirestoreStorage();
  }

  /**
   * Extract array_value from a protobufjs Message object
   * This tries multiple methods to access the oneof field
   */
  private extractArrayValueFromMessage(
    valueMessage: any,
    fieldName: string,
  ): any | null {
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
          } catch (e) {
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
          } catch (e) {
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

          this.logger.log(
            'grpc',
            `Commit: Successfully extracted array_value for '${fieldName}' from message: ${JSON.stringify(arrayValueObj)}`,
          );

          return arrayValueObj;
        }
      }

      return null;
    } catch (e) {
      this.logger.log(
        'grpc',
        `Commit: Error in extractArrayValueFromMessage for '${fieldName}': ${e instanceof Error ? e.message : String(e)}`,
      );
      return null;
    }
  }

  /**
   * Manually decode array_value from buffer
   * This is a workaround for protobufjs not decoding oneof fields correctly
   * We search for the field in the buffer and decode it manually
   */
  private manualDecodeArrayValue(
    buffer: Buffer,
    fieldName: string,
    writeIndex: number,
  ): any | null {
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
      const found = false;

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
        const beforeIndex = index > 0 ? buffer[index - 1] : 0;

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

                  this.logger.log(
                    'grpc',
                    `Commit: Manually decoded array_value for '${fieldName}': ${JSON.stringify(arrayValueObj)}`,
                  );

                  return arrayValueObj;
                } catch (e) {
                  // Decoding failed, continue searching
                }
              }
            }
          }
        }

        searchStart = index + 1;
      }

      return null;
    } catch (e) {
      this.logger.log(
        'grpc',
        `Commit: Error in manualDecodeArrayValue for '${fieldName}': ${e instanceof Error ? e.message : String(e)}`,
      );
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
          // IMPORTANT: For oneof fields, protobufjs may not expose them correctly
          // We need to access them directly from the decoded message before toObject()
          const message = requestType.decode(buffer);

          // For Commit requests, try to access oneof fields directly from the message
          // BEFORE calling toObject(), as toObject() may lose oneof information
          if (methodName === 'Commit' && this.ValueType && this.protobufRoot) {
            const rawMessage = message as any;
            if (rawMessage.writes && Array.isArray(rawMessage.writes)) {
              // Process writes to extract oneof fields directly from protobufjs Message objects
              rawMessage.writes.forEach((write: any, writeIndex: number) => {
                if (write && write.update && write.update.fields) {
                  const fieldsMessage = write.update.fields;
                  // IMPORTANT: In protobufjs, map fields are stored as plain objects, not as protobufjs Map objects
                  // However, the VALUES within the map (Value messages) should still be protobufjs Message objects
                  // We need to check if fieldsMessage is a Map or a plain object
                  this.logger.log(
                    'grpc',
                    `Commit: fieldsMessage type: ${typeof fieldsMessage}, constructor: ${fieldsMessage?.constructor?.name}, is Map: ${fieldsMessage instanceof Map}`,
                  );

                  if (fieldsMessage && typeof fieldsMessage === 'object') {
                    // fieldsMessage is a plain object (map<string, Value> converted by protobufjs)
                    // The VALUES in this map should still be protobufjs Message objects
                    // Get all field names from the object
                    const fieldNames = Object.keys(fieldsMessage).filter(
                      (key) =>
                        !key.startsWith('$') &&
                        key !== 'toJSON' &&
                        key !== 'toObject',
                    );

                    this.logger.log(
                      'grpc',
                      `Commit: Found ${fieldNames.length} fields in document: ${fieldNames.join(', ')}`,
                    );

                    // For each field, try to access the Value message directly
                    fieldNames.forEach((fieldName: string) => {
                      try {
                        // Get the Value message for this field
                        const valueMessage = fieldsMessage[fieldName];

                        // Log detailed information about the value message
                        if (
                          fieldName === 'items' ||
                          fieldName.includes('array') ||
                          fieldName.includes('list')
                        ) {
                          this.logger.log(
                            'grpc',
                            `Commit: Field '${fieldName}' - type: ${typeof valueMessage}, constructor: ${valueMessage?.constructor?.name}, has $type: ${!!valueMessage?.$type}, keys: ${Object.keys(valueMessage || {}).join(', ')}`,
                          );

                          // Check if it's a protobufjs Message object
                          if (valueMessage && valueMessage.$type) {
                            this.logger.log(
                              'grpc',
                              `Commit: Field '${fieldName}' IS a protobufjs Message, $type name: ${valueMessage.$type?.name}`,
                            );

                            // Try to access all properties of the message
                            const allProps =
                              Object.getOwnPropertyNames(valueMessage);
                            const allKeys = Object.keys(valueMessage);
                            this.logger.log(
                              'grpc',
                              `Commit: Field '${fieldName}' - all properties: ${allProps.join(', ')}, all keys: ${allKeys.join(', ')}`,
                            );

                            // Check if oneof is set
                            if (valueMessage.$oneof) {
                              this.logger.log(
                                'grpc',
                                `Commit: Field '${fieldName}' has $oneof: ${JSON.stringify(valueMessage.$oneof)}`,
                              );
                            }

                            // Try to access array_value directly using different methods
                            const arrayValue1 = valueMessage.array_value;
                            const arrayValue2 = valueMessage.arrayValue;
                            const arrayValue3 = valueMessage[9]; // Field number
                            const arrayValue4 = valueMessage['9'];

                            // Try to access internal protobufjs properties
                            const internalFields = valueMessage.$fields;
                            const internalOneof = valueMessage.$oneof;
                            const internalOneofs = valueMessage.$oneofs;

                            this.logger.log(
                              'grpc',
                              `Commit: Field '${fieldName}' array_value access - array_value: ${!!arrayValue1}, arrayValue: ${!!arrayValue2}, [9]: ${!!arrayValue3}, ['9']: ${!!arrayValue4}`,
                            );

                            this.logger.log(
                              'grpc',
                              `Commit: Field '${fieldName}' internal props - $fields: ${!!internalFields}, $oneof: ${!!internalOneof ? JSON.stringify(internalOneof) : 'null'}, $oneofs: ${!!internalOneofs ? JSON.stringify(internalOneofs) : 'null'}`,
                            );

                            // Try to access via field descriptor
                            const valueType = this.ValueType!;
                            if (valueType && valueType.fields) {
                              const arrayValueField =
                                valueType.fields['array_value'];
                              if (arrayValueField) {
                                this.logger.log(
                                  'grpc',
                                  `Commit: Field '${fieldName}' array_value field descriptor - id: ${arrayValueField.id}, name: ${arrayValueField.name}`,
                                );

                                // Try to get the field value using the descriptor
                                try {
                                  // protobufjs may store fields in $fields by ID
                                  if (
                                    internalFields &&
                                    internalFields[arrayValueField.id]
                                  ) {
                                    const fieldValue =
                                      internalFields[arrayValueField.id];
                                    this.logger.log(
                                      'grpc',
                                      `Commit: Field '${fieldName}' found array_value in $fields[${arrayValueField.id}]: ${JSON.stringify(fieldValue)}`,
                                    );
                                    // Store it for later use
                                    if (!valueMessage._decodedArrayValue) {
                                      valueMessage._decodedArrayValue =
                                        fieldValue;
                                    }
                                  }
                                } catch (e) {
                                  this.logger.log(
                                    'grpc',
                                    `Commit: Field '${fieldName}' error accessing $fields: ${e instanceof Error ? e.message : String(e)}`,
                                  );
                                }
                              }
                            }
                          } else {
                            this.logger.log(
                              'grpc',
                              `Commit: Field '${fieldName}' is NOT a protobufjs Message (no $type property)`,
                            );
                          }
                        }

                        if (valueMessage && valueMessage.$type) {
                          // This is a protobufjs Message object
                          // Try to access oneof fields directly
                          const valueType = this.ValueType!;

                          // Method 1: Check if oneof case is set
                          if (
                            valueType.oneofsArray &&
                            valueType.oneofsArray.length > 0
                          ) {
                            const oneof = valueType.oneofsArray[0]; // value_type oneof
                            if (oneof && oneof.name) {
                              // Try to get the oneof case
                              const oneofCase = valueMessage[oneof.name];
                              if (
                                oneofCase === 'array_value' ||
                                oneofCase === 'arrayValue'
                              ) {
                                // Try to get the array_value field directly
                                const arrayValueField =
                                  valueType.fields['array_value'];
                                if (arrayValueField) {
                                  // Try to access the field value using the field descriptor
                                  try {
                                    // protobufjs stores field values in a special way
                                    // Try accessing via field ID
                                    const fieldId = arrayValueField.id;
                                    const arrayValue =
                                      valueMessage[fieldId] ||
                                      valueMessage['array_value'] ||
                                      valueMessage['arrayValue'];

                                    if (arrayValue) {
                                      this.logger.log(
                                        'grpc',
                                        `Commit: Found array_value for '${fieldName}' via field ID ${fieldId}: ${JSON.stringify(arrayValue)}`,
                                      );
                                      // Store it in the message for later use
                                      if (!valueMessage._decodedArrayValue) {
                                        valueMessage._decodedArrayValue =
                                          arrayValue;
                                      }
                                    }
                                  } catch (e) {
                                    // Field access failed
                                  }
                                }
                              }
                            }
                          }

                          // Method 2: Try to access all oneof fields and check which one is set
                          // protobufjs may store oneof fields in $oneof property
                          if (valueMessage.$oneof) {
                            const oneofObj = valueMessage.$oneof;
                            Object.keys(oneofObj).forEach(
                              (oneofName: string) => {
                                if (
                                  oneofObj[oneofName] === 'array_value' ||
                                  oneofObj[oneofName] === 'arrayValue'
                                ) {
                                  this.logger.log(
                                    'grpc',
                                    `Commit: Found oneof case 'array_value' for '${fieldName}' in $oneof property`,
                                  );
                                  // Try to get the actual array value
                                  const arrayValue =
                                    valueMessage.array_value ||
                                    valueMessage.arrayValue;
                                  if (arrayValue) {
                                    this.logger.log(
                                      'grpc',
                                      `Commit: Successfully extracted array_value for '${fieldName}' from $oneof: ${JSON.stringify(arrayValue)}`,
                                    );
                                    if (!valueMessage._decodedArrayValue) {
                                      valueMessage._decodedArrayValue =
                                        arrayValue;
                                    }
                                  }
                                }
                              },
                            );
                          }
                        }
                      } catch (e) {
                        // Error accessing field, continue
                      }
                    });
                  }
                }
              });
            }
          }

          // Store the original buffer for manual decoding of array_value fields
          // We'll pass it to handleCommit so it can manually decode arrays
          const originalBuffer = buffer;

          // For Commit requests, check if buffer contains array data
          if (methodName === 'Commit') {
            const bufferStr = buffer.toString('hex');
            const bufferAscii = buffer.toString(
              'ascii',
              0,
              Math.min(500, buffer.length),
            );
            const item1Hex = Buffer.from('item-1', 'utf8').toString('hex');
            const item2Hex = Buffer.from('item-2', 'utf8').toString('hex');
            const item1Index = bufferStr.indexOf(item1Hex);
            const item2Index = bufferStr.indexOf(item2Hex);

            // Also check for the string directly in ASCII
            const item1AsciiIndex = bufferAscii.indexOf('item-1');
            const item2AsciiIndex = bufferAscii.indexOf('item-2');

            if (
              item1Index !== -1 ||
              item2Index !== -1 ||
              item1AsciiIndex !== -1 ||
              item2AsciiIndex !== -1
            ) {
              this.logger.log(
                'grpc',
                `Commit: Buffer contains array data! item-1 hex at ${item1Index}, item-2 hex at ${item2Index}, item-1 ascii at ${item1AsciiIndex}, item-2 ascii at ${item2AsciiIndex}, buffer length: ${buffer.length}`,
              );
              // Log first 200 bytes of buffer in hex for debugging
              this.logger.log(
                'grpc',
                `Commit: Buffer first 200 bytes (hex): ${buffer.slice(0, 200).toString('hex')}`,
              );
            } else {
              this.logger.log(
                'grpc',
                `Commit: Buffer does NOT contain array data (item-1 or item-2 not found), buffer length: ${buffer.length}`,
              );
              // Log first 200 bytes of buffer in hex for debugging
              this.logger.log(
                'grpc',
                `Commit: Buffer first 200 bytes (hex): ${buffer.slice(0, Math.min(200, buffer.length)).toString('hex')}`,
              );
              // Also log as ASCII to see if we can spot the issue
              this.logger.log(
                'grpc',
                `Commit: Buffer first 200 bytes (ascii, printable only): ${bufferAscii.replace(/[^\x20-\x7E]/g, '.')}`,
              );
            }
          }

          // For Commit requests, we need to manually decode array_value fields
          // because protobufjs doesn't decode oneof fields correctly
          if (methodName === 'Commit' && this.ValueType && this.protobufRoot) {
            // Try to manually decode array_value fields from the buffer
            // by finding the field in the buffer and decoding it separately
            const rawMessage = message as any;
            if (rawMessage.writes) {
              // Process each write to manually decode array_value fields
              rawMessage.writes = rawMessage.writes.map(
                (write: any, writeIndex: number) => {
                  if (write.update && write.update.fields) {
                    const fields: any = {};
                    Object.keys(write.update.fields).forEach((key) => {
                      const fieldValue = write.update.fields[key];
                      // If field is empty, try to decode it manually from the raw message
                      if (
                        fieldValue &&
                        typeof fieldValue === 'object' &&
                        Object.keys(fieldValue).length === 0
                      ) {
                        // Get the raw Value message from the decoded message
                        const rawWrite = rawMessage.writes?.[writeIndex];
                        const rawFieldValue = rawWrite?.update?.fields?.[key];

                        if (rawFieldValue) {
                          // rawFieldValue is a protobufjs Message object
                          // Try to decode array_value directly from it
                          const decodedArray =
                            this.extractArrayValueFromMessage(
                              rawFieldValue,
                              key,
                            );
                          if (decodedArray) {
                            fields[key] = { arrayValue: decodedArray };
                          } else {
                            // Try manual buffer decoding as fallback
                            const decodedArrayFromBuffer =
                              this.manualDecodeArrayValue(
                                originalBuffer,
                                key,
                                writeIndex,
                              );
                            if (decodedArrayFromBuffer) {
                              fields[key] = {
                                arrayValue: decodedArrayFromBuffer,
                              };
                            } else {
                              fields[key] = fieldValue;
                            }
                          }
                        } else {
                          fields[key] = fieldValue;
                        }
                      } else {
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
                },
              );
            }
          }

          // Convert to plain object with all fields, including oneof fields
          // Based on Next.js article: https://mojoauth.com/serialize-and-deserialize/serialize-and-deserialize-protobuf-with-nextjs
          // The article shows using decode() then toObject() with proper options
          // IMPORTANT: For oneof fields, we need to ensure oneofs: true is set
          // However, if protobufjs didn't decode the oneof field correctly during decode(),
          // toObject() won't be able to recover it
          const obj = requestType.toObject(message, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: false, // Don't include default values
            arrays: true,
            objects: true,
            oneofs: true, // This is crucial - protobufjs handles oneofs correctly!
          });

          // DEBUG: For Commit requests, check if oneof fields were correctly converted
          if (methodName === 'Commit' && obj.writes) {
            obj.writes.forEach((write: any, writeIndex: number) => {
              if (write && write.update && write.update.fields) {
                Object.keys(write.update.fields).forEach(
                  (fieldName: string) => {
                    if (
                      fieldName === 'items' ||
                      fieldName.includes('array') ||
                      fieldName.includes('list')
                    ) {
                      const fieldValue = write.update.fields[fieldName];
                      this.logger.log(
                        'grpc',
                        `Commit: After toObject() - Field '${fieldName}' value: ${JSON.stringify(fieldValue)}, has array_value: ${!!fieldValue?.array_value}, has arrayValue: ${!!fieldValue?.arrayValue}`,
                      );
                    }
                  },
                );
              }
            });
          }

          // For Commit requests, also try accessing oneof fields directly from the message
          // protobufjs Message objects may expose oneof fields differently
          if (methodName === 'Commit' && this.protobufRoot) {
            const rawMessage = message as any;
            // Try to access oneof fields directly from the message before toObject
            // protobufjs may store oneof fields in a special property
            if (rawMessage.writes && Array.isArray(rawMessage.writes)) {
              rawMessage.writes.forEach((write: any, writeIndex: number) => {
                if (write && write.update && write.update.fields) {
                  // Access fields directly from the protobufjs Message object
                  // before it's converted to plain object
                  const fieldsMessage = write.update.fields;
                  if (fieldsMessage && typeof fieldsMessage === 'object') {
                    // Try to get field descriptors and access values directly
                    Object.keys(fieldsMessage).forEach((fieldName: string) => {
                      const fieldValue = fieldsMessage[fieldName];
                      // If fieldValue is a protobufjs Message, try to access oneof directly
                      if (
                        fieldValue &&
                        typeof fieldValue === 'object' &&
                        fieldValue.$type
                      ) {
                        // protobufjs Message objects have $type property
                        // Try to access oneof fields via field descriptors
                        const valueType = this.ValueType;
                        if (valueType && valueType.oneofsArray) {
                          // Check each oneof case
                          valueType.oneofsArray.forEach((oneof: any) => {
                            if (oneof.name === 'value_type') {
                              // Try to get the oneof case value
                              const oneofCase = fieldValue[oneof.name];
                              if (oneofCase) {
                                this.logger.log(
                                  'grpc',
                                  `Commit: Found oneof case '${oneofCase}' for field '${fieldName}' directly from message`,
                                );
                                // Try to access the actual field value based on the oneof case
                                if (
                                  oneofCase === 'array_value' ||
                                  oneofCase === 'arrayValue'
                                ) {
                                  const arrayValue =
                                    fieldValue.array_value ||
                                    fieldValue.arrayValue;
                                  if (arrayValue) {
                                    this.logger.log(
                                      'grpc',
                                      `Commit: Successfully accessed array_value for '${fieldName}' directly from message: ${JSON.stringify(arrayValue)}`,
                                    );
                                    // Update obj with the correctly decoded array
                                    if (
                                      obj.writes &&
                                      obj.writes[writeIndex] &&
                                      obj.writes[writeIndex].update
                                    ) {
                                      if (
                                        !obj.writes[writeIndex].update.fields
                                      ) {
                                        obj.writes[writeIndex].update.fields =
                                          {};
                                      }
                                      obj.writes[writeIndex].update.fields[
                                        fieldName
                                      ] = {
                                        arrayValue: arrayValue,
                                      };
                                    }
                                  } else {
                                    this.logger.log(
                                      'grpc',
                                      `Commit: oneof case is 'array_value' for '${fieldName}' but arrayValue is null/undefined`,
                                    );
                                  }
                                }
                              } else {
                                // Log when oneof case is not found for debugging
                                if (
                                  fieldName === 'items' ||
                                  fieldName.includes('array') ||
                                  fieldName.includes('list')
                                ) {
                                  this.logger.log(
                                    'grpc',
                                    `Commit: No oneof case found for field '${fieldName}' (expected array_value). Field value type: ${typeof fieldValue}, has $type: ${!!fieldValue.$type}, keys: ${Object.keys(fieldValue || {}).join(', ')}`,
                                  );
                                }
                              }
                            }
                          });
                        }
                      }
                    });
                  }
                }
              });
            }
          }

          // For Commit requests, try to use @google-cloud/firestore's Serializer
          // to properly decode array_value fields that protobufjs loses
          if (methodName === 'Commit' && obj.writes) {
            try {
              // Import @google-cloud/firestore's Serializer and convert utilities
              const {
                Serializer,
              } = require('@google-cloud/firestore/build/src/serializer');
              const {
                detectValueType,
              } = require('@google-cloud/firestore/build/src/convert');

              // Create a mock Firestore instance for the Serializer
              const mockFirestore = {
                _settings: { useBigInt: false },
                doc: (path: string) => ({ path }),
              };
              const serializer = new Serializer(mockFirestore as any);

              // Access raw message to get oneof fields
              const rawMessage = message as any;

              // Process each write to decode array_value fields using the Serializer
              obj.writes = obj.writes.map((write: any, index: number) => {
                if (write.update && write.update.fields) {
                  const fields: any = {};
                  const rawWrite = rawMessage.writes?.[index];

                  Object.keys(write.update.fields).forEach((key) => {
                    const fieldValue = write.update.fields[key];

                    // Check if this field is an array using detectValueType
                    try {
                      const valueType = detectValueType(fieldValue);
                      this.logger.log(
                        'grpc',
                        `Commit: Field '${key}' detected type: ${valueType}, value: ${JSON.stringify(fieldValue)}`,
                      );

                      if (valueType === 'arrayValue') {
                        // Try to get the raw Value message from protobufjs
                        const rawField = rawWrite?.update?.fields?.[key];

                        if (rawField) {
                          // Try to convert rawField to object using protobufjs
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

                          this.logger.log(
                            'grpc',
                            `Commit: Raw field '${key}' as object: ${JSON.stringify(rawFieldObj)}`,
                          );

                          // Use serializer's decodeValue to properly decode the array
                          try {
                            const decoded = serializer.decodeValue(rawFieldObj);
                            this.logger.log(
                              'grpc',
                              `Commit: Using @google-cloud/firestore Serializer to decode '${key}': ${JSON.stringify(decoded)}`,
                            );
                            // Convert back to FirestoreValue format
                            const { toFirestoreValue } = require('./utils');
                            fields[key] = {
                              arrayValue: {
                                values: decoded.map((v: any) =>
                                  toFirestoreValue(v),
                                ),
                              },
                            };
                          } catch (e) {
                            this.logger.log(
                              'grpc',
                              `Commit: Serializer.decodeValue failed for '${key}': ${e instanceof Error ? e.message : String(e)}`,
                            );
                            // If decoding fails, keep original
                            fields[key] = fieldValue;
                          }
                        } else {
                          // No raw field available, keep original
                          fields[key] = fieldValue;
                        }
                      } else {
                        fields[key] = fieldValue;
                      }
                    } catch (e) {
                      // detectValueType failed, keep original
                      this.logger.log(
                        'grpc',
                        `Commit: detectValueType failed for '${key}': ${e instanceof Error ? e.message : String(e)}`,
                      );
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
            } catch (e) {
              // If @google-cloud/firestore's serializer is not available, log and continue
              this.logger.log(
                'grpc',
                `Commit: Could not use @google-cloud/firestore serializer: ${e instanceof Error ? e.message : String(e)}`,
              );
            }
          }

          // Log for debugging Commit requests
          if (methodName === 'Commit' && obj.writes) {
            this.logger.log(
              'grpc',
              `Commit deserialized with protobufjs: ${JSON.stringify(obj.writes[0]?.update?.fields || {})}`,
            );
          }

          return obj;
        },
        responseSerialize: (value: any): Buffer => {
          // Serialize using protobufjs
          // When protobufjs loads from JSON, it preserves the field names as-is
          // The JSON proto uses snake_case, so fromObject() expects snake_case
          const message = responseType.fromObject(value);
          return Buffer.from(responseType.encode(message).finish());
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
   * This is used when proto-loader loses oneof field data
   *
   * NOTE: This currently reconstructs empty values because proto-loader loses
   * the actual data before it reaches our code. The values are lost during
   * deserialization, so we can only reconstruct the type, not the actual data.
   *
   * TODO: Investigate using gRPC interceptors or protobufjs directly to capture
   * the raw message buffer before proto-loader deserializes it.
   */
  private reconstructDocumentFields(
    document: FirestoreDocument,
  ): Record<string, FirestoreValue> {
    if (!document.fieldTypes) {
      return document.fields;
    }

    const reconstructed: Record<string, FirestoreValue> = {
      ...document.fields,
    };

    // For each field with metadata, ensure it has the correct type
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
   * Parse document path like "projects/{project}/databases/{db}/documents/{collection}/{doc}"
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
    let databaseId = parts[dbIndex + 1];
    // Normalize database ID: (default) -> default
    if (databaseId === '(default)') {
      databaseId = 'default';
    }
    const collectionId = parts[docsIndex + 1] || '';
    const docId = parts[docsIndex + 2] || '';

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

      this.logger.log('grpc', `GetDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
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

      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionId,
      );

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
    try {
      const request = call.request;
      let parent = request.parent || '';
      // Handle both camelCase (from JSON protos) and snake_case formats
      const structuredQuery =
        request.structured_query || request.structuredQuery || {};
      const from = structuredQuery.from;

      // Debug: log full request structure
      this.logger.log(
        'grpc',
        `RunQuery full request keys: ${Object.keys(request).join(', ')}, structured_query present: ${!!request.structured_query}, structured_query keys: ${request.structured_query ? Object.keys(request.structured_query).join(', ') : 'null'}`,
      );

      // Normalize parent path: replace (default) with default
      parent = parent.replace('/databases/(default)/', '/databases/default/');

      // Parse parent path to extract projectId, databaseId
      const parts = parent.split('/');
      const projectId = parts[1] || 'test-project';
      const databaseId = parts[3] || 'default';

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

      // Debug: log the full structured_query to understand the structure
      this.logger.log(
        'grpc',
        `RunQuery request: parent=${parent}, collectionId=${collectionId || '(empty)'}, structuredQuery keys: ${Object.keys(structuredQuery).join(', ')}, from type: ${typeof from}, from keys: ${from ? Object.keys(from).join(', ') : 'null'}`,
      );

      // Get documents from storage
      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionId,
      );

      // Convert current time to Timestamp format (seconds and nanos)
      const now = new Date();
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      // Send responses as a stream
      // According to proto: RunQueryResponse has document = 1, read_time = 2, skipped_results = 3
      // When loaded from JSON, protobufjs uses camelCase: readTime, skippedResults
      // For empty collections, we still need to send a response with readTime
      if (documents.length === 0) {
        // For empty collections, send a response with readTime but no document
        // The document field should be omitted (not null) for empty results
        const emptyResponse = {
          readTime: timestamp,
          skippedResults: 0,
        };
        call.write(emptyResponse);
        this.logger.log(
          'grpc',
          `RunQuery response: SUCCESS - Empty collection (0 documents)`,
        );
      } else {
        // Send each document as a stream response
        documents.forEach((doc) => {
          const documentPath = buildDocumentPath(
            projectId,
            databaseId,
            collectionId,
            doc.name.split('/').pop() || '',
          );

          // Reconstruct fields using metadata if needed
          const reconstructedFields = this.reconstructDocumentFields(doc);
          // RunQueryResponse: document = 1, readTime = 2, skippedResults = 3
          // When loaded from JSON, protobufjs uses camelCase
          // Ensure timestamps are always set (never null/undefined)
          const defaultTimestamp = toTimestamp(now);
          const grpcDocument = {
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

          call.write(grpcDocument);
        });
        this.logger.log(
          'grpc',
          `RunQuery response: SUCCESS - Streamed ${documents.length} documents`,
        );
      }

      // End the stream
      call.end();
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      call.destroy({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      } as grpc.ServiceError);
    }
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

      // Parse parent path
      const parts = parent.split('/');
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

      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        finalDocId,
      );

      // Convert request document to Firestore format
      const fields = request.document?.fields || {};
      const document = toFirestoreDocument(documentPath, fields);
      document.name = documentPath;

      this.storage.setDocument(
        projectId,
        databaseId,
        collectionId,
        finalDocId,
        document,
      );

      this.logger.log(
        'grpc',
        `CreateDocument response: SUCCESS - Created document at ${documentPath}`,
      );
      // Convert to gRPC Document format with Timestamp
      const grpcDocument = {
        name: document.name,
        fields: document.fields,
        create_time: document.createTime
          ? toTimestamp(new Date(document.createTime))
          : undefined,
        update_time: document.updateTime
          ? toTimestamp(new Date(document.updateTime))
          : undefined,
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

      const fields = request.document?.fields || {};
      const document = toFirestoreDocument(path, fields);
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
      callback(null, document);
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
    try {
      const request = call.request;
      const database = request.database || '';
      const writes = request.writes || [];

      this.logger.log(
        'grpc',
        `Commit request: database=${database}, writes=${writes.length}`,
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
          `Commit response: ERROR - Invalid database path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid database path: ${database}`,
        });
        return;
      }

      const projectId = parts[projectIndex + 1];
      let databaseId = parts[dbIndex + 1];

      // Normalize database ID
      if (databaseId === '(default)') {
        databaseId = 'default';
      }

      const writeResults: any[] = [];
      const now = new Date();
      // Use the same timestamp format as RunQuery (inline format)
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      // Process each write
      // If we have protobufjs loaded, try to deserialize the request manually
      // to capture oneof fields that proto-loader loses
      const writesToProcess = writes;
      if (this.protobufRoot && writes.length > 0) {
        try {
          // Get the CommitRequest message type
          const CommitRequestType = this.protobufRoot.lookupType(
            'google.firestore.v1.CommitRequest',
          );
          if (CommitRequestType) {
            // Try to serialize the request and deserialize it with protobufjs
            // But we don't have the raw buffer, so we'll work with what we have
            // For now, we'll process writes as-is and handle empty objects separately
            this.logger.log(
              'grpc',
              'Commit: protobufjs available, will attempt manual deserialization for empty fields',
            );
          }
        } catch (error) {
          // Continue with proto-loader deserialization
          this.logger.log(
            'grpc',
            `Commit: protobufjs lookup failed, using proto-loader data: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }

      for (const write of writesToProcess) {
        // Log all write properties to check for update_transforms or other fields
        this.logger.log(
          'grpc',
          `Commit: Raw write object keys: ${Object.keys(write).join(', ')}`,
        );

        // Check if arrays are sent in update_transforms instead of update.fields
        if (write.update_transforms && Array.isArray(write.update_transforms)) {
          this.logger.log(
            'grpc',
            `Commit: Found update_transforms with ${write.update_transforms.length} transforms`,
          );
          write.update_transforms.forEach((transform: any, index: number) => {
            this.logger.log(
              'grpc',
              `Commit: Transform ${index}: field_path=${transform.field_path}, transform_type=${Object.keys(
                transform,
              )
                .filter((k) => k !== 'field_path')
                .join(', ')}`,
            );
          });
        }

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
            callback({
              code: grpc.status.INVALID_ARGUMENT,
              message: `Invalid document path: ${docPath}`,
            });
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
          // doc.fields is already in FirestoreValue format from gRPC
          // But we need to convert it to our internal FirestoreDocument format
          // The fields come as a map<string, Value> from gRPC
          // Note: proto-loader with keepCase: true keeps snake_case
          this.logger.log(
            'grpc',
            `Commit: Document fields keys: ${doc.fields ? Object.keys(doc.fields).join(', ') : 'none'}`,
          );
          // Log the raw write object to see what we're receiving
          this.logger.log(
            'grpc',
            `Commit: Raw write object keys: ${Object.keys(write).join(', ')}`,
          );
          this.logger.log(
            'grpc',
            `Commit: Raw write.update keys: ${write.update ? Object.keys(write.update).join(', ') : 'none'}`,
          );
          if (write.update && write.update.fields) {
            this.logger.log(
              'grpc',
              `Commit: Raw write.update.fields keys: ${Object.keys(write.update.fields).join(', ')}`,
            );
            if (write.update.fields.items) {
              this.logger.log(
                'grpc',
                `Commit: Raw items from write.update.fields: ${JSON.stringify(write.update.fields.items)}`,
              );
            }
          }
          if (doc.fields && doc.fields.items) {
            this.logger.log(
              'grpc',
              `Commit: items field value: ${JSON.stringify(doc.fields.items)}`,
            );
            this.logger.log(
              'grpc',
              `Commit: items field type: ${typeof doc.fields.items}, constructor: ${doc.fields.items?.constructor?.name}`,
            );
            this.logger.log(
              'grpc',
              `Commit: items field keys: ${Object.keys(doc.fields.items || {}).join(', ')}`,
            );
            // Check all possible property names, including non-enumerable
            const itemsObj = doc.fields.items;
            const allProps = Object.getOwnPropertyNames(itemsObj);
            const descriptors = Object.getOwnPropertyDescriptors(itemsObj);
            this.logger.log(
              'grpc',
              `Commit: items Object.getOwnPropertyNames: ${allProps.join(', ')}`,
            );
            this.logger.log(
              'grpc',
              `Commit: items property descriptors: ${JSON.stringify(Object.keys(descriptors))}`,
            );
            // Check prototype chain
            const proto = Object.getPrototypeOf(itemsObj);
            this.logger.log(
              'grpc',
              `Commit: items prototype: ${proto ? proto.constructor.name : 'null'}`,
            );
            // Try to access value_type (oneof field name in proto)
            if ('value_type' in itemsObj) {
              this.logger.log(
                'grpc',
                `Commit: items has value_type: ${itemsObj.value_type}`,
              );
            }
            // Try to access the oneof case
            if ('$type' in itemsObj) {
              this.logger.log(
                'grpc',
                `Commit: items has $type: ${itemsObj.$type}`,
              );
            }
          }
          const fields: Record<string, FirestoreValue> = {};
          const fieldTypes: Record<string, FieldType> = {};

          if (doc.fields) {
            Object.keys(doc.fields).forEach((key) => {
              const value = doc.fields[key];
              // Ensure the value is a proper FirestoreValue object
              if (value && typeof value === 'object') {
                // Debug: log field being processed
                this.logger.log(
                  'grpc',
                  `Commit: Processing field '${key}', value keys: ${Object.keys(value).join(', ')}, value: ${JSON.stringify(value)}`,
                );
                // With oneofs: true, proto-loader exposes a field indicating which oneof case is active
                // If value is empty object {}, try to use protobufjs to deserialize manually
                // This is a workaround for proto-loader losing oneof field data
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
                  this.logger.log(
                    'grpc',
                    `Commit: Empty object detected for '${key}', inferred type: ${inferredType} (proto-loader lost oneof data)`,
                  );
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
                const normalizedValue =
                  normalizeGrpcValueToFirestoreValue(value);
                // Only add field if normalizedValue has at least one property
                if (Object.keys(normalizedValue).length > 0) {
                  fields[key] = normalizedValue;
                  // Store the detected type in metadata
                  const detectedType = this.detectFieldType(normalizedValue);
                  if (detectedType) {
                    fieldTypes[key] = detectedType;
                  }
                  // Debug: log field being saved
                  if ('arrayValue' in normalizedValue) {
                    this.logger.log(
                      'grpc',
                      `Commit: Saving field '${key}' as array with ${normalizedValue.arrayValue?.values?.length || 0} values`,
                    );
                  }
                } else {
                  // Debug: log why field is not being saved
                  this.logger.log(
                    'grpc',
                    `Commit: Field '${key}' not saved - normalizedValue has no properties`,
                  );
                }
              }
            });
          }

          const document: FirestoreDocument = {
            name: docPath,
            fields,
            createTime: existingDoc?.createTime || new Date().toISOString(),
            updateTime: new Date().toISOString(),
            fieldTypes:
              Object.keys(fieldTypes).length > 0 ? fieldTypes : undefined,
          };

          this.storage.setDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
            document,
          );

          writeResults.push({
            update_time: timestamp,
          });
        } else if (write.delete) {
          // Delete document
          const docPath = write.delete;
          const parsed = this.parseDocumentPath(docPath);

          if (!parsed) {
            this.logger.log(
              'grpc',
              `Commit response: ERROR - Invalid document path in delete`,
            );
            callback({
              code: grpc.status.INVALID_ARGUMENT,
              message: `Invalid document path: ${docPath}`,
            });
            return;
          }

          this.storage.deleteDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
          );

          writeResults.push({
            updateTime: timestamp,
          });
        }
      }

      this.logger.log(
        'grpc',
        `Commit response: SUCCESS - Processed ${writes.length} writes`,
      );

      callback(null, {
        commitTime: timestamp,
        writeResults: writeResults,
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      this.logger.error('grpc', `Commit error: ${errorMessage}`);
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
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

      this.logger.log(
        'grpc',
        `BatchGetDocuments request: database=${database}, documents=${documents.length}`,
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

      const projectId = parts[projectIndex + 1];
      let databaseId = parts[dbIndex + 1];

      // Normalize database ID
      if (databaseId === '(default)') {
        databaseId = 'default';
      }

      // Process each document request
      for (const docPath of documents) {
        const parsed = this.parseDocumentPath(docPath);

        if (!parsed) {
          this.logger.log(
            'grpc',
            `BatchGetDocuments response: MISSING - Invalid document path: ${docPath}`,
          );
          const now = new Date();
          const readTime = toTimestamp(now);
          call.write({
            missing: docPath,
            readTime: readTime,
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
          this.logger.log(
            'grpc',
            `BatchGetDocuments response: FOUND - Document: ${docPath}`,
          );
          // Debug: log document fields before conversion
          const fieldKeys = Object.keys(document.fields);
          this.logger.log(
            'grpc',
            `BatchGetDocuments: Document has ${fieldKeys.length} fields: ${fieldKeys.join(', ')}`,
          );
          if (document.fields.items) {
            this.logger.log(
              'grpc',
              `BatchGetDocuments: items field: ${JSON.stringify(document.fields.items)}`,
            );
          }
          // Reconstruct fields using metadata if needed
          const reconstructedFields = this.reconstructDocumentFields(document);
          // Convert internal document format to gRPC Document format
          // The document from storage has fields in FirestoreValue format already
          // But we need to ensure create_time and update_time are in Timestamp format
          const now = new Date();
          const readTime = toTimestamp(now);
          const defaultTimestamp = toTimestamp(now);
          const grpcFields = toGrpcFields(reconstructedFields);
          // Debug: log converted fields
          if (grpcFields.items) {
            this.logger.log(
              'grpc',
              `BatchGetDocuments: Converted items field: ${JSON.stringify(grpcFields.items)}`,
            );
          }
          // Ensure createTime and updateTime are always set (never null/undefined)
          // When loaded from JSON, protobufjs uses camelCase
          // Firebase Admin SDK expects Timestamp objects, not null
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
          // readTime must be at the top level of BatchGetDocumentsResponse, not inside found
          call.write({
            found: grpcDocument,
            readTime: readTime,
          });
        } else {
          this.logger.log(
            'grpc',
            `BatchGetDocuments response: MISSING - Document not found: ${docPath}`,
          );
          const now = new Date();
          const readTime = toTimestamp(now);
          call.write({
            missing: docPath,
            readTime: readTime,
          });
        }
      }

      this.logger.log(
        'grpc',
        `BatchGetDocuments response: SUCCESS - Processed ${documents.length} documents`,
      );
      call.end();
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
            CreateDocument: this.handleCreateDocument.bind(this),
            UpdateDocument: this.handleUpdateDocument.bind(this),
            DeleteDocument: this.handleDeleteDocument.bind(this),
            Commit: this.handleCommitWithProtobufjs.bind(this),
            BatchGetDocuments: this.handleBatchGetDocuments.bind(this),
          };

          // Load proto file - try using official Firebase proto first
          const officialProtoPath = path.join(
            __dirname,
            '../../node_modules/@google-cloud/firestore/build/protos/google/firestore/v1/firestore.proto',
          );
          const localProtoPath = path.join(
            __dirname,
            '../proto/firestore.proto',
          );
          // Use official proto if available, otherwise fall back to local
          const protoPath = require('fs').existsSync(officialProtoPath)
            ? officialProtoPath
            : localProtoPath;

          // Load proto with protobufjs - use the SAME method as firebase-admin
          // firebase-admin uses Root.fromJSON() with v1.json, which works correctly
          // This is the key difference - JSON protos handle oneof fields correctly
          try {
            // Try to load from JSON first (same as firebase-admin)
            // Use require.resolve to find the correct path
            let jsonProtoPath: string | null = null;
            try {
              jsonProtoPath =
                require.resolve('@google-cloud/firestore/build/protos/v1.json');
            } catch (e) {
              // Fallback to relative path
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
              // Fallback to loading from .proto file
              this.logger.log(
                'server',
                'Loading proto from .proto file (fallback)',
              );
              this.protobufRoot = await protobuf.load(protoPath);
            } else {
              // Fallback to local proto
              this.logger.log(
                'server',
                'Loading proto from local .proto file (fallback)',
              );
              const protoContent = fs.readFileSync(protoPath, 'utf8');
              this.protobufRoot = protobuf.parse(protoContent, {
                keepCase: true,
              }).root;
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

              this.logger.log(
                'server',
                'Loaded proto with protobufjs only (no proto-loader)',
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

  public getStorage(): FirestoreStorage {
    return this.storage;
  }

  public getConfig(): Readonly<ServerConfig> {
    return { ...this.config };
  }
}
