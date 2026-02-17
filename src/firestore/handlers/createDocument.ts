/**
 * CreateDocument gRPC handler
 */

import * as grpc from '@grpc/grpc-js';
import { FirestoreValue } from '../../types';
import type { FirestoreServer } from '../server';
import {
  buildDocumentPath,
  generateDocumentId,
  toTimestamp,
  toGrpcFields,
  normalizeGrpcValueToFirestoreValue,
} from '../utils';

export function handleCreateDocument(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const request = call.request;
    const parent = request.parent || '';
    const collectionId = request.collectionId || '';
    const docId = request.documentId || 'auto-generated';

    server.logger.log(
      'grpc',
      `CreateDocument request: parent=${parent}, collectionId=${collectionId}, documentId=${docId}`,
    );

    const parts = parent.split('/');
    const projectIndex = parts.indexOf('projects');
    const dbIndex = parts.indexOf('databases');
    const docsIndex = parts.indexOf('documents');

    if (
      projectIndex === -1 ||
      dbIndex === -1 ||
      projectIndex + 1 >= parts.length ||
      dbIndex + 1 >= parts.length
    ) {
      server.logger.log(
        'grpc',
        `CreateDocument response: ERROR - Invalid parent path`,
      );
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid parent path: ${parent}`,
      });
      return;
    }

    const projectId = parts[projectIndex + 1];
    const databaseId = parts[dbIndex + 1];
    const finalDocId = request.documentId || generateDocumentId();

    const pathAfterDocuments =
      docsIndex >= 0 ? parts.slice(docsIndex + 1).join('/') : '';
    const collectionPath = pathAfterDocuments
      ? `${pathAfterDocuments}/${collectionId}`
      : collectionId;

    const documentPath = buildDocumentPath(
      projectId,
      databaseId,
      collectionPath,
      finalDocId,
    );

    const rawFields = request.document?.fields || {};
    const fields: Record<string, FirestoreValue> = {};
    Object.keys(rawFields).forEach((key) => {
      fields[key] = normalizeGrpcValueToFirestoreValue(rawFields[key]);
    });
    const nowStr = new Date().toISOString();
    const document = {
      name: documentPath,
      fields,
      createTime: nowStr,
      updateTime: nowStr,
    };

    server
      .getStorage()
      .setDocument(projectId, databaseId, collectionPath, finalDocId, document);

    server.logger.log(
      'grpc',
      `CreateDocument response: SUCCESS - Created document at ${documentPath}`,
    );
    const now = new Date();
    const defaultTimestamp = toTimestamp(now);
    const grpcDocument = {
      name: document.name,
      fields: toGrpcFields(document.fields),
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
