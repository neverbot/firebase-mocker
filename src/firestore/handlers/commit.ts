/**
 * Commit gRPC handler (write operations: set, add, update, delete)
 */

import * as grpc from '@grpc/grpc-js';
import { FirestoreDocument, FirestoreValue, FieldType } from '../../types';
import type { FirestoreServer } from '../server';
import { normalizeGrpcValueToFirestoreValue } from '../utils';

interface NumericRead {
  kind: 'int' | 'double';
  value: number;
}

function readNumeric(value: any): NumericRead | null {
  if (!value || typeof value !== 'object') {
    return null;
  }
  if ('integerValue' in value || 'integer_value' in value) {
    const raw = value.integerValue ?? value.integer_value;
    const n = parseInt(String(raw), 10);
    if (Number.isNaN(n)) {
      return null;
    }
    return { kind: 'int', value: n };
  }
  if ('doubleValue' in value || 'double_value' in value) {
    const raw = value.doubleValue ?? value.double_value;
    const n = Number(raw);
    if (Number.isNaN(n)) {
      return null;
    }
    return { kind: 'double', value: n };
  }
  return null;
}

function encodeNumeric(kind: 'int' | 'double', value: number): FirestoreValue {
  if (kind === 'int') {
    return { integerValue: String(Math.trunc(value)) };
  }
  return { doubleValue: value };
}

function canonicalizeValue(v: any): any {
  if (v === null || typeof v !== 'object') {
    return v;
  }
  if (Array.isArray(v)) {
    return v.map(canonicalizeValue);
  }
  const out: Record<string, any> = {};
  const keys = Object.keys(v).sort();
  for (const key of keys) {
    if (key === 'valueType' || key === 'value_type') {
      continue;
    }
    out[key] = canonicalizeValue(v[key]);
  }
  return out;
}

function valuesEqual(a: any, b: any): boolean {
  return (
    JSON.stringify(canonicalizeValue(a)) ===
    JSON.stringify(canonicalizeValue(b))
  );
}

function getExistingArrayValues(existing: FirestoreValue | undefined): any[] {
  if (!existing) {
    return [];
  }
  const av = (existing as any).arrayValue;
  if (av && Array.isArray(av.values)) {
    return av.values;
  }
  return [];
}

/**
 * Apply a single FieldTransform to an existing value and return the new
 * FirestoreValue, or null if the transform is not supported / not numeric/array.
 * The caller should fall back to its own logic (e.g. serverTimestamp) for null.
 */
function applyFieldTransform(
  transform: any,
  existingValue: FirestoreValue | undefined,
): FirestoreValue | null {
  const numericOp = (
    operand: any,
    op: 'inc' | 'max' | 'min',
  ): FirestoreValue | null => {
    const operandNum = readNumeric(operand);
    if (!operandNum) {
      return null;
    }
    const baseNum = readNumeric(existingValue) ?? {
      kind: 'int' as const,
      value: 0,
    };
    const resultKind: 'int' | 'double' =
      baseNum.kind === 'double' || operandNum.kind === 'double'
        ? 'double'
        : 'int';
    let resultValue: number;
    if (op === 'inc') {
      resultValue = baseNum.value + operandNum.value;
    } else if (op === 'max') {
      resultValue = Math.max(baseNum.value, operandNum.value);
    } else {
      resultValue = Math.min(baseNum.value, operandNum.value);
    }
    return encodeNumeric(resultKind, resultValue);
  };

  if (transform.increment !== undefined) {
    return numericOp(transform.increment, 'inc');
  }
  if (transform.maximum !== undefined) {
    return numericOp(transform.maximum, 'max');
  }
  if (transform.minimum !== undefined) {
    return numericOp(transform.minimum, 'min');
  }

  const append =
    transform.appendMissingElements ?? transform.append_missing_elements;
  if (append !== undefined) {
    const incoming = Array.isArray(append.values) ? append.values : [];
    const current = getExistingArrayValues(existingValue);
    const merged = current.map(canonicalizeValue);
    for (const v of incoming) {
      const canonical = canonicalizeValue(v);
      if (!merged.some((m) => valuesEqual(m, canonical))) {
        merged.push(canonical);
      }
    }
    return { arrayValue: { values: merged } };
  }

  const removeArr =
    transform.removeAllFromArray ?? transform.remove_all_from_array;
  if (removeArr !== undefined) {
    const toRemove = Array.isArray(removeArr.values) ? removeArr.values : [];
    const current = getExistingArrayValues(existingValue);
    const filtered = current
      .map(canonicalizeValue)
      .filter((c) => !toRemove.some((r: any) => valuesEqual(r, c)));
    return { arrayValue: { values: filtered } };
  }

  return null;
}

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

        const processTransform = (t: any): void => {
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
            return;
          }

          const existingValue = existingDoc?.fields?.[fieldPath];
          const transformed = applyFieldTransform(t, existingValue);
          if (transformed !== null) {
            fields[fieldPath] = transformed;
            const detectedType = server.detectFieldType(transformed);
            if (detectedType) {
              fieldTypes[fieldPath] = detectedType;
            }
          }
        };

        const updateTransforms =
          write.updateTransforms || write.update_transforms;
        if (Array.isArray(updateTransforms)) {
          updateTransforms.forEach(processTransform);
        }

        const transform = write.transform;
        const fieldTransforms =
          transform &&
          (transform.fieldTransforms || transform.field_transforms);
        if (Array.isArray(fieldTransforms)) {
          fieldTransforms.forEach(processTransform);
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

    // If the request was part of a transaction, end it now (Level 1: no buffering, just cleanup)
    const txnBytes = (call.request as { transaction?: Buffer | string })
      .transaction;
    if (txnBytes) {
      const txnId = Buffer.isBuffer(txnBytes)
        ? txnBytes.toString('utf8')
        : String(txnBytes);
      server.getStorage().endTransaction(txnId);
      server.logger.log('grpc', `[Commit] ended transaction txnId=${txnId}`);
    }

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
