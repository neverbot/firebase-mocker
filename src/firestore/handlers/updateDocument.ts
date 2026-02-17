/**
 * UpdateDocument gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import {
  toFirestoreDocument,
  toTimestamp,
  toGrpcFields,
  normalizeGrpcValueToFirestoreValue,
} from '../utils';
import type { FirestoreServer } from '../server';

export function handleUpdateDocument(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const path = request.document?.name || '';

    server.logger.log('grpc', `UpdateDocument request: path=${path}`);

    const parsed = server.parseDocumentPath(path);
    if (!parsed) {
      server.logger.log(
        'grpc',
        `UpdateDocument response: ERROR - Invalid document path`,
      );
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid document path: ${path}`,
      });
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

    const rawFields = request.document?.fields || {};
    const normalizedFields: Record<string, any> = {};
    Object.keys(rawFields).forEach((key) => {
      normalizedFields[key] = normalizeGrpcValueToFirestoreValue(
        rawFields[key],
      );
    });
    const document = toFirestoreDocument(path, normalizedFields);
    document.name = path;

    if (existingDoc) {
      document.createTime = existingDoc.createTime;
    }
    document.updateTime = new Date().toISOString();

    server
      .getStorage()
      .setDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
        document,
      );

    server.logger.log(
      'grpc',
      `UpdateDocument response: SUCCESS - ${existingDoc ? 'Updated' : 'Created'} document at ${path}`,
    );
    const grpcDocument = {
      name: document.name,
      fields: toGrpcFields(document.fields),
      createTime: document.createTime
        ? toTimestamp(new Date(document.createTime))
        : toTimestamp(new Date()),
      updateTime: document.updateTime
        ? toTimestamp(new Date(document.updateTime))
        : toTimestamp(new Date()),
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
