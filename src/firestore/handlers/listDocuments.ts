/**
 * ListDocuments gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import { buildDocumentPath } from '../utils';
import type { FirestoreServer } from '../server';

export function handleListDocuments(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const parent = request.parent || '';
    const collectionId = request.collectionId || '';

    server.logger.log(
      'grpc',
      `ListDocuments request: parent=${parent}, collectionId=${collectionId}`,
    );

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
      server.logger.log(
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
      server.logger.log(
        'grpc',
        `ListDocuments response: ERROR - collectionId required`,
      );
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'collectionId is required',
      });
      return;
    }

    const pathAfterDocuments = parts.slice(docsIndex + 1).join('/');
    const collectionPath = pathAfterDocuments
      ? `${pathAfterDocuments}/${collectionId}`
      : collectionId;

    const documents = server
      .getStorage()
      .listDocuments(projectId, databaseId, collectionPath);

    for (const doc of documents) {
      const docId = (doc.name && doc.name.split('/').pop()) || (doc as any).id;
      if (docId) {
        doc.name = buildDocumentPath(
          projectId,
          databaseId,
          collectionPath,
          docId,
        );
      }
    }

    server.logger.log(
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
