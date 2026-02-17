/**
 * RunAggregationQuery gRPC handler (e.g. for count()).
 * Server streaming; supports COUNT aggregation only; sum/avg return 0.
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleRunAggregationQuery(
  server: FirestoreServer,
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
            if (seen.has(v)) {
              return undefined;
            }
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
        if (from.collection_id) {
          collectionId = from.collection_id;
        } else if (from.collectionId) {
          collectionId = from.collectionId;
        } else if (typeof from === 'object' && Object.keys(from).length > 0) {
          const first = from[Object.keys(from)[0]];
          collectionId = first?.collection_id || first?.collectionId || '';
        } else if (Array.isArray(from) && from.length > 0) {
          collectionId = from[0]?.collection_id || from[0]?.collectionId || '';
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

      let documents = server
        .getStorage()
        .listDocuments(projectId, databaseId, collectionPath);
      if (where) {
        documents = server.applyQueryFilters(documents, where);
      }

      const count = documents.length;
      const now = new Date();
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      const aggregateFields: Record<
        string,
        { integerValue?: string; doubleValue?: number }
      > = {};
      for (const agg of Array.isArray(aggregations) ? aggregations : []) {
        const alias =
          agg.alias ??
          agg.Alias ??
          `alias_${Object.keys(aggregateFields).length}`;
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
      const msg = error instanceof Error ? error.message : String(error);
      call.destroy({
        code: grpc.status.INTERNAL,
        message: `RunAggregationQuery failed: ${msg}`,
      } as grpc.ServiceError);
    }
  });
}
