/**
 * RunQuery gRPC handler (server streaming)
 * Used by collection.get() in Firebase Admin SDK
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';
import { buildDocumentPath, toTimestamp, toGrpcFields } from '../utils';

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
      if (from) {
        if (from.collection_id) {
          collectionId = from.collection_id;
        } else if (from.collectionId) {
          collectionId = from.collectionId;
        } else if (typeof from === 'object') {
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
          const firstFrom = from[0];
          if (firstFrom) {
            collectionId =
              firstFrom.collection_id || firstFrom.collectionId || '';
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

      let documents = server
        .getStorage()
        .listDocuments(projectId, databaseId, collectionPath);

      server.logger.log(
        'grpc',
        `RunQuery DEBUG: Found ${documents.length} documents in collection before filtering`,
      );

      if (where) {
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
              const docId = (doc.name && doc.name.split('/').pop()) || '';
              const documentPath = buildDocumentPath(
                projectId,
                databaseId,
                collectionPath,
                docId,
              );
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
