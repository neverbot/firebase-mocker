/**
 * gRPC server that emulates Firestore API
 */

import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
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
  private packageDefinition?: protoLoader.PackageDefinition;

  constructor(config: ServerConfig) {
    this.config = config;
    this.storage = new FirestoreStorage();
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
      const grpcDocument = {
        name: document.name,
        fields: toGrpcFields(reconstructedFields),
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
      const structuredQuery = request.structured_query || {};
      const from = structuredQuery.from;

      // Normalize parent path: replace (default) with default
      parent = parent.replace('/databases/(default)/', '/databases/default/');

      // Parse parent path to extract projectId, databaseId
      const parts = parent.split('/');
      const projectId = parts[1] || 'test-project';
      const databaseId = parts[3] || 'default';

      // Get collection ID from the query
      // The from field can be an array of CollectionSelector objects
      let collectionId = '';
      if (from) {
        if (Array.isArray(from) && from.length > 0) {
          const firstFrom = from[0];
          if (firstFrom && firstFrom.collection_id) {
            collectionId = firstFrom.collection_id;
          }
        } else if (from.collection_id) {
          // Handle case where from is a single object
          collectionId = from.collection_id;
        }
      }

      this.logger.log(
        'grpc',
        `RunQuery request: parent=${parent}, collectionId=${collectionId || '(empty)'}`,
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
      if (documents.length === 0) {
        // For empty collections, send a response with readTime but no document
        const emptyResponse = {
          read_time: timestamp,
          skipped_results: 0,
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
          // RunQueryResponse: document = 1, read_time = 2, skipped_results = 3
          const grpcDocument = {
            document: {
              name: documentPath,
              fields: toGrpcFields(reconstructedFields),
              create_time: doc.createTime
                ? toTimestamp(new Date(doc.createTime))
                : undefined,
              update_time: doc.updateTime
                ? toTimestamp(new Date(doc.updateTime))
                : undefined,
            },
            read_time: timestamp,
            skipped_results: 0,
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
              let value = doc.fields[key];
              // Ensure the value is a proper FirestoreValue object
              if (value && typeof value === 'object') {
                // Debug: log field being processed
                this.logger.log(
                  'grpc',
                  `Commit: Processing field '${key}', value keys: ${Object.keys(value).join(', ')}, value: ${JSON.stringify(value)}`,
                );
                // With oneofs: true, proto-loader exposes a field indicating which oneof case is active
                // If value is empty object {}, try to access oneof fields using the value_type indicator
                // Based on Stack Overflow: https://stackoverflow.com/questions/78129497/grpc-fails-to-serialize-object-with-arbitrary-properties
                // When oneof fields are used, values must be wrapped in the correct oneof object structure
                if (Object.keys(value).length === 0) {
                  // Try to access all possible oneof field names
                  // With oneofs: true, proto-loader may expose fields differently
                  const possibleArrayFields = [
                    value.array_value,
                    value.arrayValue,
                    value.array_value?.values,
                    value.arrayValue?.values,
                  ];

                  let foundArrayValue = null;
                  for (const arrayField of possibleArrayFields) {
                    if (
                      arrayField &&
                      typeof arrayField === 'object' &&
                      arrayField.values
                    ) {
                      foundArrayValue = arrayField;
                      break;
                    }
                  }

                  if (foundArrayValue) {
                    this.logger.log(
                      'grpc',
                      `Commit: Found array_value for '${key}' via direct access: ${JSON.stringify(foundArrayValue)}`,
                    );
                    value = { array_value: foundArrayValue };
                  } else {
                    // Check for value_type indicator (works for other fields but not arrays)
                    const valueType = value.value_type || value.valueType;
                    this.logger.log(
                      'grpc',
                      `Commit: Empty object detected for '${key}', value_type: ${valueType}, no array_value found`,
                    );

                    // Infer field type from context (heuristic: if field name suggests array, assume array)
                    const inferredType: FieldType = this.inferFieldType(key);
                    this.logger.log(
                      'grpc',
                      `Commit: Storing '${key}' with inferred type: ${inferredType} (proto-loader lost oneof data)`,
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
            update_time: timestamp,
          });
        }
      }

      this.logger.log(
        'grpc',
        `Commit response: SUCCESS - Processed ${writes.length} writes`,
      );

      callback(null, {
        commit_time: timestamp,
        write_results: writeResults,
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
            read_time: readTime,
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
          const grpcFields = toGrpcFields(reconstructedFields);
          // Debug: log converted fields
          if (grpcFields.items) {
            this.logger.log(
              'grpc',
              `BatchGetDocuments: Converted items field: ${JSON.stringify(grpcFields.items)}`,
            );
          }
          const grpcDocument = {
            name: document.name,
            fields: grpcFields,
            create_time: document.createTime
              ? toTimestamp(new Date(document.createTime))
              : undefined,
            update_time: document.updateTime
              ? toTimestamp(new Date(document.updateTime))
              : undefined,
          };
          // read_time must be at the top level of BatchGetDocumentsResponse, not inside found
          call.write({
            found: grpcDocument,
            read_time: readTime,
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
            read_time: readTime,
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
      try {
        this.grpcServer = new grpc.Server();

        // Create service implementation
        const serviceImplementation: any = {
          GetDocument: this.handleGetDocument.bind(this),
          ListDocuments: this.handleListDocuments.bind(this),
          RunQuery: this.handleRunQuery.bind(this),
          CreateDocument: this.handleCreateDocument.bind(this),
          UpdateDocument: this.handleUpdateDocument.bind(this),
          DeleteDocument: this.handleDeleteDocument.bind(this),
          Commit: this.handleCommit.bind(this),
          BatchGetDocuments: this.handleBatchGetDocuments.bind(this),
        };

        // Load proto file - try using official Firebase proto first
        const officialProtoPath = path.join(
          __dirname,
          '../../node_modules/@google-cloud/firestore/build/protos/google/firestore/v1/firestore.proto',
        );
        const localProtoPath = path.join(__dirname, '../proto/firestore.proto');
        // Use official proto if available, otherwise fall back to local
        const protoPath = require('fs').existsSync(officialProtoPath)
          ? officialProtoPath
          : localProtoPath;

        this.packageDefinition = protoLoader.loadSync(protoPath, {
          keepCase: true,
          longs: String,
          enums: String,
          defaults: true,
          oneofs: true, // With oneofs: true, proto-loader exposes a field indicating which oneof case is active
        });

        const firestoreProto = grpc.loadPackageDefinition(
          this.packageDefinition,
        ) as any;

        // Get the Firestore service
        const firestoreService =
          firestoreProto.google?.firestore?.v1?.Firestore;

        if (!firestoreService) {
          reject(
            new Error('Failed to load Firestore service from proto definition'),
          );
          return;
        }

        // Add service to server using the loaded proto definition
        this.grpcServer.addService(
          firestoreService.service,
          serviceImplementation,
        );

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
