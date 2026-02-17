/**
 * ListCollectionIds gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleListCollectionIds(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const parent = request.parent || '';

    server.logger.log('grpc', `[ListCollectionIds] parent=${parent}`);

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
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid parent path: ${parent}`,
      });
      return;
    }

    const projectId = parts[projectIndex + 1];
    const databaseId = parts[dbIndex + 1];
    const pathAfterDocuments =
      docsIndex + 1 < parts.length ? parts.slice(docsIndex + 1).join('/') : '';

    const collectionIds = server
      .getStorage()
      .listCollectionIds(projectId, databaseId, pathAfterDocuments);

    const pageSize = request.pageSize || 0;
    const pageToken = request.pageToken || '';
    let resultIds = collectionIds;
    let nextPageToken = '';

    if (pageSize > 0) {
      const start = pageToken ? parseInt(pageToken, 10) || 0 : 0;
      const end = start + pageSize;
      resultIds = collectionIds.slice(start, end);
      if (end < collectionIds.length) {
        nextPageToken = String(end);
      }
    }

    server.logger.log(
      'grpc',
      `[ListCollectionIds] returning ${resultIds.length} ids: ${resultIds.join(', ')}`,
    );

    callback(null, {
      collectionIds: resultIds,
      nextPageToken,
    });
  } catch (err) {
    server.logger.log(
      'grpc',
      `[ListCollectionIds] error: ${err instanceof Error ? err.message : String(err)}`,
    );
    callback(
      {
        code: grpc.status.INTERNAL,
        message: err instanceof Error ? err.message : String(err),
      },
      null,
    );
  }
}
