/**
 * GetDocument gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import { toTimestamp, toGrpcFields } from '../utils';
import type { FirestoreServer } from '../server';

export function handleGetDocument(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const path = request.name || '';

    server.logger.log('grpc', `[GetDocument] Called with path=${path}`);
    server.logger.log(
      'grpc',
      `[GetDocument] Request object: ${JSON.stringify(request).substring(0, 200)}`,
    );

    const parsed = server.parseDocumentPath(path);

    if (parsed) {
      server.logger.log(
        'grpc',
        `GetDocument DEBUG: Looking for document with projectId=${parsed.projectId}, databaseId=${parsed.databaseId}, collectionId=${parsed.collectionId}, docId=${parsed.docId}`,
      );
    }
    if (!parsed) {
      server.logger.log(
        'grpc',
        `GetDocument response: ERROR - Invalid document path`,
      );
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid document path: ${path}`,
      });
      return;
    }

    const document = server
      .getStorage()
      .getDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

    if (!document) {
      server.logger.log(
        'grpc',
        `GetDocument response: NOT_FOUND - Document not found`,
      );
      callback({
        code: grpc.status.NOT_FOUND,
        message: `Document not found: ${path}`,
      });
      return;
    }

    server.logger.log('grpc', `GetDocument response: SUCCESS - Document found`);
    const reconstructedFields = server.reconstructDocumentFields(document);
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
