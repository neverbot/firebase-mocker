/**
 * Commit gRPC handler (write operations: set, add, update, delete)
 */

import * as grpc from '@grpc/grpc-js';
import { FirestoreDocument, FirestoreValue, FieldType } from '../../types';
import { normalizeGrpcValueToFirestoreValue } from '../../utils';
import type { FirestoreServer } from '../server';

export function handleCommitWithProtobufjs(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  handleCommit(server, call, callback);
}

export function handleCommit(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  let callbackInvoked = false;
  const safeCallback: grpc.sendUnaryData<any> = (err, value) => {
    if (callbackInvoked) {
      return;
    }
    callbackInvoked = true;
    callback(err, value);
  };

  try {
    server.logger.log('grpc', '[Commit] Called');
    const request = call.request;
    const database = request.database || '';
    const rawWrites = request.writes;
    const writes = Array.isArray(rawWrites)
      ? rawWrites
      : rawWrites && typeof rawWrites === 'object'
        ? Object.values(rawWrites)
        : [];

    server.logger.log(
      'grpc',
      `[Commit] Request: database=${database}, writes count=${writes.length}`,
    );

    const parts = database.split('/');
    const projectIndex = parts.indexOf('projects');
    const dbIndex = parts.indexOf('databases');

    if (
      projectIndex === -1 ||
      dbIndex === -1 ||
      projectIndex + 1 >= parts.length ||
      dbIndex + 1 >= parts.length
    ) {
      server.logger.log(
        'grpc',
        `Commit: ${writes.length} writes - ERROR: Invalid database path`,
      );
      safeCallback(
        {
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid database path: ${database}`,
        },
        null,
      );
      return;
    }

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
        if (deletePath !== null && deletePath !== undefined) {
          docPath =
            typeof deletePath === 'string' ? deletePath : String(deletePath);
          operation = 'delete';
        }
      }

      if (docPath) {
        const parsed = server.parseDocumentPath(docPath);
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

    const logParts: string[] = [];
    for (const [collection, ops] of writesByCollection.entries()) {
      const partsArr: string[] = [];
      if (ops.updates.length > 0) {
        partsArr.push(`+${ops.updates.join(',')}`);
      }
      if (ops.deletes.length > 0) {
        partsArr.push(`-${ops.deletes.join(',')}`);
      }
      if (partsArr.length > 0) {
        logParts.push(`${collection}[${partsArr.join(' ')}]`);
      }
    }
    const compactLog = logParts.join(' ');

    server.logger.log(
      'grpc',
      `Commit: ${compactLog || `${writes.length} writes`}`,
    );

    const writeResults: any[] = [];
    const now = new Date();
    const timestamp = {
      seconds: Math.floor(now.getTime() / 1000),
      nanos: (now.getTime() % 1000) * 1000000,
    };

    for (const write of writes) {
      if (write.update) {
        const doc = write.update;
        const docPath = doc.name || '';

        const parsed = server.parseDocumentPath(docPath);

        if (!parsed) {
          server.logger.log(
            'grpc',
            `Commit response: ERROR - Invalid document path in write`,
          );
          safeCallback(
            {
              code: grpc.status.INVALID_ARGUMENT,
              message: `Invalid document path: ${docPath}`,
            },
            null,
          );
          return;
        }

        const existingDoc = server
          .getStorage()
          .getDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
          );

        const fields: Record<string, FirestoreValue> = {};
        const fieldTypes: Record<string, FieldType> = {};

        let fieldsSource: Record<string, any> = doc.fields || {};
        if (
          fieldsSource &&
          typeof (fieldsSource as any).toObject === 'function'
        ) {
          try {
            fieldsSource = (fieldsSource as any).toObject({
              longs: String,
              enums: String,
              bytes: String,
              defaults: true,
              oneofs: true,
            });
          } catch {
            fieldsSource = doc.fields || {};
          }
        }
        if (fieldsSource && typeof fieldsSource === 'object') {
          Object.keys(fieldsSource).forEach((key) => {
            let value = fieldsSource[key];
            if (
              value &&
              typeof value === 'object' &&
              typeof value.toObject === 'function'
            ) {
              try {
                value = value.toObject({
                  longs: String,
                  enums: String,
                  bytes: String,
                  defaults: true,
                  oneofs: true,
                });
              } catch {
                // keep value as-is
              }
            }
            if (value && typeof value === 'object') {
              const normalizedValue = normalizeGrpcValueToFirestoreValue(value);
              if (normalizedValue && Object.keys(normalizedValue).length > 0) {
                fields[key] = normalizedValue;
                const detectedType = server.detectFieldType(normalizedValue);
                if (detectedType) {
                  fieldTypes[key] = detectedType;
                }
                return;
              }
              if (Object.keys(value).length === 0) {
                if (server.protobufRoot && write.update) {
                  try {
                    const ValueType = server.protobufRoot.lookupType(
                      'google.firestore.v1.Value',
                    );
                    if (ValueType) {
                      const rawField = write.update.fields?.[key];
                      if (rawField) {
                        server.logger.log(
                          'grpc',
                          `Commit: Attempting protobufjs deserialization for empty field '${key}'`,
                        );
                      }
                    }
                  } catch (error) {
                    server.logger.log(
                      'grpc',
                      `Commit: protobufjs deserialization failed for '${key}': ${error instanceof Error ? error.message : String(error)}`,
                    );
                  }
                }

                const inferredType: FieldType = server.inferFieldType(key);
                fieldTypes[key] = inferredType;
                if (inferredType === 'arrayValue') {
                  fields[key] = { arrayValue: { values: [] } };
                } else if (inferredType === 'mapValue') {
                  fields[key] = { mapValue: { fields: {} } };
                } else {
                  fields[key] = { nullValue: null };
                }
                return;
              }
              const normalizedValueFallback =
                normalizeGrpcValueToFirestoreValue(value);
              if (Object.keys(normalizedValueFallback).length > 0) {
                fields[key] = normalizedValueFallback;
                const detectedType = server.detectFieldType(
                  normalizedValueFallback,
                );
                if (detectedType) {
                  fieldTypes[key] = detectedType;
                }
              }
            }
          });
        }

        const updateTransforms =
          write.updateTransforms || write.update_transforms;
        if (Array.isArray(updateTransforms)) {
          updateTransforms.forEach((t: any) => {
            if (!t) {
              return;
            }
            const fieldPath =
              t.fieldPath || t.field_path || t.field || t.Field || '';
            if (!fieldPath) {
              return;
            }
            const serverValue =
              t.setToServerValue || t.set_to_server_value || t.serverValue;
            if (
              serverValue === 'REQUEST_TIME' ||
              serverValue === 1 ||
              serverValue === 'REQUEST_TIME_UNSPECIFIED'
            ) {
              const iso = now.toISOString();
              fields[fieldPath] = { timestampValue: iso };
              const detectedType = server.detectFieldType(fields[fieldPath]);
              if (detectedType) {
                fieldTypes[fieldPath] = detectedType;
              }
            }
          });
        }

        const transform = write.transform;
        const fieldTransforms =
          transform &&
          (transform.fieldTransforms || transform.field_transforms);
        if (Array.isArray(fieldTransforms)) {
          fieldTransforms.forEach((t: any) => {
            if (!t) {
              return;
            }
            const fieldPath =
              t.fieldPath || t.field_path || t.field || t.Field || '';
            if (!fieldPath) {
              return;
            }
            const serverValue =
              t.setToServerValue || t.set_to_server_value || t.serverValue;
            if (
              serverValue === 'REQUEST_TIME' ||
              serverValue === 1 ||
              serverValue === 'REQUEST_TIME_UNSPECIFIED'
            ) {
              const iso = now.toISOString();
              fields[fieldPath] = { timestampValue: iso };
              const detectedType = server.detectFieldType(fields[fieldPath]);
              if (detectedType) {
                fieldTypes[fieldPath] = detectedType;
              }
            }
          });
        }

        let finalFields = fields;
        if (existingDoc && write.updateMask) {
          const mask = write.updateMask;
          const fieldPaths: string[] =
            mask.field_paths ?? mask.fieldPaths ?? [];
          const existingFields = existingDoc.fields || {};
          finalFields = { ...existingFields, ...fields };
          for (const path of fieldPaths) {
            if (!(path in fields)) {
              delete finalFields[path];
            }
          }
        } else if (existingDoc && !write.updateMask) {
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

        server.logger.log(
          'grpc',
          `Commit DEBUG: Saving document with projectId=${parsed.projectId}, databaseId=${parsed.databaseId}, collectionId=${parsed.collectionId}, docId=${parsed.docId}`,
        );

        server
          .getStorage()
          .setDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
            document,
          );

        const savedDoc = server
          .getStorage()
          .getDocument(
            parsed.projectId,
            parsed.databaseId,
            parsed.collectionId,
            parsed.docId,
          );
        if (savedDoc) {
          server.logger.log(
            'grpc',
            `Commit DEBUG: Document verified after save - exists in storage`,
          );
        } else {
          server.logger.log(
            'grpc',
            `Commit DEBUG: WARNING - Document NOT found in storage after save!`,
          );
        }

        writeResults.push({
          update_time: timestamp,
        });
      } else {
        const deletePath = write.delete ?? write['delete'];
        if (deletePath !== null && deletePath !== undefined) {
          const docPath =
            typeof deletePath === 'string' ? deletePath : String(deletePath);
          const parsed = server.parseDocumentPath(docPath);

          if (!parsed) {
            server.logger.log(
              'grpc',
              `Commit response: ERROR - Invalid document path in delete`,
            );
            safeCallback(
              {
                code: grpc.status.INVALID_ARGUMENT,
                message: `Invalid document path: ${docPath}`,
              },
              null,
            );
            return;
          }

          server
            .getStorage()
            .deleteDocument(
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
    }

    server.logger.log(
      'grpc',
      `Commit: ${compactLog || `${writes.length} writes`} ✓`,
    );

    safeCallback(null, {
      write_results: writeResults,
      commit_time: timestamp,
    });
  } catch (error: unknown) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error';
    server.logger.error('grpc', `Commit error: ${errorMessage}`);
    safeCallback(
      {
        code: grpc.status.INTERNAL,
        message: errorMessage,
      },
      null,
    );
  }
}
