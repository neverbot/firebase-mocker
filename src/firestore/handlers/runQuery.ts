/**
 * RunQuery gRPC handler (server streaming)
 * Used by collection.get() in Firebase Admin SDK
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';
import { buildDocumentPath, toTimestamp, toGrpcFields } from '../utils';

/**
 * Detect whether a `where` clause references the implicit `__name__` field.
 * When present we treat the query as a "kindless all descendants" query (used
 * by `firestore.recursiveDelete()`) and skip the leaf-collection filter — the
 * existing fieldFilter logic handles the name range.
 */
/**
 * Walk the `where` clause and return the first collection-segment found in a
 * `__name__` reference value. Used to derive the scan prefix for kindless
 * queries where the SDK omits `from.collectionId` and encodes the scope in
 * the filter values (e.g. `projects/.../documents/events/__id-MIN__`).
 */
function extractKindlessRootCollection(where: any): string | null {
  if (!where || typeof where !== 'object') {
    return null;
  }
  const fieldFilter = where.fieldFilter ?? where.field_filter;
  if (fieldFilter) {
    const path =
      fieldFilter.field?.fieldPath ??
      fieldFilter.field?.field_path ??
      fieldFilter.Field?.fieldPath ??
      fieldFilter.Field?.field_path;
    if (path === '__name__') {
      const val = fieldFilter.value;
      const ref = val?.referenceValue ?? val?.reference_value;
      if (typeof ref === 'string') {
        const match = /\/documents\/([^/]+)/.exec(ref);
        if (match) {
          return match[1];
        }
      }
    }
  }
  const composite = where.compositeFilter ?? where.composite_filter;
  if (composite) {
    const filters = composite.filters ?? [];
    if (Array.isArray(filters)) {
      for (const f of filters) {
        const found = extractKindlessRootCollection(f);
        if (found) {
          return found;
        }
      }
    }
  }
  return null;
}

function whereHasNameFilter(where: any): boolean {
  if (!where || typeof where !== 'object') {
    return false;
  }
  const fieldFilter = where.fieldFilter ?? where.field_filter;
  if (fieldFilter) {
    const fieldPath =
      fieldFilter.field?.fieldPath ??
      fieldFilter.field?.field_path ??
      fieldFilter.Field?.fieldPath ??
      fieldFilter.Field?.field_path;
    if (fieldPath === '__name__') {
      return true;
    }
  }
  const composite = where.compositeFilter ?? where.composite_filter;
  if (composite) {
    const filters = composite.filters ?? [];
    if (Array.isArray(filters)) {
      return filters.some((f) => whereHasNameFilter(f));
    }
  }
  return false;
}

export function handleRunQuery(
  server: FirestoreServer,
  call: grpc.ServerWritableStream<any, any>,
): void {
  const rawRequest = call.request;
  setImmediate(() => {
    try {
      let request: any;
      try {
        const seen = new WeakSet<object>();
        const json = JSON.stringify(rawRequest, (k, v) => {
          try {
            if (v !== null && typeof v === 'object') {
              if (seen.has(v)) {
                return undefined;
              }
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
      let transactionId: string | undefined;
      const reqNewTxn =
        (request as { newTransaction?: unknown; new_transaction?: unknown })
          .newTransaction ||
        (request as { newTransaction?: unknown; new_transaction?: unknown })
          .new_transaction;
      if (reqNewTxn) {
        transactionId = server.getStorage().createTransaction();
        server.logger.log(
          'grpc',
          `[RunQuery] implicit BeginTransaction txnId=${transactionId}`,
        );
      }
      let isFirstResponse = true;

      const parent = request.parent || '';
      const structuredQuery =
        request.structured_query || request.structuredQuery || {};
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
      let allDescendants = false;
      const readFromSelector = (sel: any): void => {
        if (!sel) {
          return;
        }
        collectionId = sel.collection_id || sel.collectionId || collectionId;
        const desc = sel.all_descendants ?? sel.allDescendants;
        if (desc === true) {
          allDescendants = true;
        }
      };
      if (from) {
        if (Array.isArray(from)) {
          if (from.length > 0) {
            readFromSelector(from[0]);
          }
        } else if (from.collection_id || from.collectionId) {
          readFromSelector(from);
        } else if (typeof from === 'object') {
          const keys = Object.keys(from);
          if (keys.length > 0) {
            readFromSelector(from[keys[0]]);
          }
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

      if (where) {
        server.logger.log(
          'grpc',
          `RunQuery DEBUG: Query has filter/where clause: ${JSON.stringify(where).substring(0, 500)}`,
        );
      } else {
        server.logger.log(
          'grpc',
          `RunQuery DEBUG: Query has NO filter/where clause`,
        );
      }

      server.logger.log(
        'grpc',
        `RunQuery DEBUG: Querying collection with projectId=${projectId}, databaseId=${databaseId}, collectionPath=${collectionPath}`,
      );

      let documents;
      let kindless = false;
      if (allDescendants) {
        // Kindless query (`firestore.recursiveDelete`): the SDK omits
        // `from.collectionId`. For a CollectionReference target the scope is
        // encoded as a `__name__` range filter on the where clause; for a
        // DocumentReference target the scope is `request.parent` (the doc
        // path) and there is no where clause at all.
        const hasNameRangeFilter = whereHasNameFilter(where);
        kindless = hasNameRangeFilter || !collectionId;
        const kindlessRoot = hasNameRangeFilter
          ? extractKindlessRootCollection(where)
          : null;
        const scanPrefix = pathAfterDocuments
          ? `${pathAfterDocuments}/`
          : kindlessRoot
            ? `${kindlessRoot}/`
            : '';
        const all = server
          .getStorage()
          .listAllDocumentsWithPath(projectId, databaseId);
        documents = all
          .filter(({ collectionPath: cp }) => {
            if (!scanPrefix) {
              return true;
            }
            return cp === scanPrefix.slice(0, -1) || cp.startsWith(scanPrefix);
          })
          .filter(({ collectionPath: cp }) => {
            if (kindless) {
              return true;
            }
            const segments = cp.split('/');
            return segments[segments.length - 1] === collectionId;
          })
          .map(({ doc }) => doc);
      } else {
        documents = server
          .getStorage()
          .listDocuments(projectId, databaseId, collectionPath);
      }

      server.logger.log(
        'grpc',
        `RunQuery DEBUG: Found ${documents.length} documents in collection before filtering`,
      );

      if (where && !kindless) {
        documents = server.applyQueryFilters(documents, where);
        server.logger.log(
          'grpc',
          `RunQuery DEBUG: Found ${documents.length} documents after filtering`,
        );
      }

      const orderBy =
        structuredQuery.order_by ??
        structuredQuery.orderBy ??
        structuredQuery.OrderBy;
      const orderByLen = orderBy
        ? Array.isArray(orderBy)
          ? orderBy.length
          : Object.keys(orderBy).length
        : 0;
      if (orderByLen > 0) {
        documents = server.applyOrderBy(documents, orderBy);
      }

      const startAt =
        structuredQuery.start_at ??
        structuredQuery.startAt ??
        structuredQuery.StartAt;
      const endAt =
        structuredQuery.end_at ??
        structuredQuery.endAt ??
        structuredQuery.EndAt;
      if ((startAt || endAt) && orderByLen > 0) {
        const before = documents.length;
        documents = server.applyCursors(documents, orderBy, startAt, endAt);
        server.logger.log(
          'grpc',
          `RunQuery DEBUG: Applied cursors (start_at=${startAt ? 'yes' : 'no'}, end_at=${endAt ? 'yes' : 'no'}); ${before} -> ${documents.length}`,
        );
      }

      let rawLimit: unknown =
        structuredQuery.limit ??
        structuredQuery.Limit ??
        structuredQuery[5] ??
        rawRequest?.structured_query?.limit ??
        rawRequest?.structuredQuery?.limit;
      let rawOffset: unknown =
        structuredQuery.offset ??
        structuredQuery.Offset ??
        structuredQuery[6] ??
        rawRequest?.structured_query?.offset ??
        rawRequest?.structuredQuery?.offset;
      if (rawLimit === undefined || rawOffset === undefined) {
        for (const [k, v] of Object.entries(structuredQuery)) {
          if (String(k).toLowerCase() === 'limit') {
            rawLimit = v;
          }
          if (String(k).toLowerCase() === 'offset') {
            rawOffset = v;
          }
        }
      }
      const toNum = (v: unknown): number => {
        if (v === null || v === undefined) {
          return 0;
        }
        if (typeof v === 'number' && !Number.isNaN(v)) {
          return Math.max(0, v);
        }
        if (typeof v === 'object' && v !== null) {
          if ('toNumber' in (v as any)) {
            return Math.max(0, (v as any).toNumber());
          }
          if ('value' in (v as any) && typeof (v as any).value === 'number') {
            return Math.max(0, (v as any).value);
          }
        }
        return Math.max(0, Number(v) || 0);
      };
      const offset = toNum(rawOffset);
      const limit = toNum(rawLimit);
      if (documents.length > 0 && (limit > 0 || offset > 0)) {
        server.logger.log(
          'grpc',
          `RunQuery DEBUG: Applying offset=${offset}, limit=${limit}`,
        );
      }
      if (offset > 0) {
        documents = documents.slice(offset);
      }
      if (limit > 0) {
        documents = documents.slice(0, limit);
      }

      const now = new Date();
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      const responses: any[] =
        documents.length === 0
          ? [
              {
                readTime: timestamp,
                skippedResults: 0,
              },
            ]
          : documents.map((doc) => {
              const storedName = doc.name ?? '';
              let documentPath: string;
              if (storedName.includes('/documents/')) {
                // For allDescendants queries the doc lives in a different
                // collection than the request's `from`, so preserve the path
                // stored at write time instead of rebuilding it from the
                // request's collectionPath.
                documentPath = storedName;
              } else {
                const docId = storedName.split('/').pop() || '';
                documentPath = buildDocumentPath(
                  projectId,
                  databaseId,
                  collectionPath,
                  docId,
                );
              }
              const reconstructedFields = server.reconstructDocumentFields(doc);
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

      for (const response of responses) {
        if (isFirstResponse && transactionId) {
          response.transaction = Buffer.from(transactionId, 'utf8');
        }
        isFirstResponse = false;
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
