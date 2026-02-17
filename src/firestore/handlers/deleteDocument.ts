/**
 * DeleteDocument gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleDeleteDocument(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const path = request.name || '';

    server.logger.log('grpc', `DeleteDocument request: path=${path}`);

    const parsed = server.parseDocumentPath(path);
    if (!parsed) {
      server.logger.log(
        'grpc',
        `DeleteDocument response: ERROR - Invalid document path`,
      );
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid document path: ${path}`,
      });
      return;
    }

    const deleted = server
      .getStorage()
      .deleteDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

    if (!deleted) {
      server.logger.log(
        'grpc',
        `DeleteDocument response: NOT_FOUND - Document not found`,
      );
      callback({
        code: grpc.status.NOT_FOUND,
        message: `Document not found: ${path}`,
      });
      return;
    }

    server.logger.log('grpc', `DeleteDocument response: SUCCESS - Deleted`);
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
