/**
 * BatchGetDocuments gRPC handler (server streaming)
 * Used by Firebase Admin SDK to get multiple documents efficiently
 */

import * as grpc from '@grpc/grpc-js';
import {
  toTimestamp,
  sanitizeGrpcFieldsForResponse,
  toGrpcFields,
} from '../../utils';
import type { FirestoreServer } from '../server';

export function handleBatchGetDocuments(
  server: FirestoreServer,
  call: grpc.ServerWritableStream<any, any>,
): void {
  try {
    const request = call.request;
    const database = request.database || '';
    const documents = request.documents || [];

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
        `BatchGetDocuments request: database=${database}, documents=${documents.length}`,
      );
      server.logger.log(
        'grpc',
        `BatchGetDocuments response: ERROR - Invalid database path`,
      );
      const error: grpc.ServiceError = {
        code: grpc.status.INVALID_ARGUMENT,
        message: `Invalid database path: ${database}`,
        name: 'InvalidArgument',
        details: `Invalid database path: ${database}`,
        metadata: new grpc.Metadata(),
      };
      call.destroy(error);
      return;
    }

    const docsByCollection = new Map<string, string[]>();
    for (const docPath of documents) {
      const parsed = server.parseDocumentPath(docPath);
      if (parsed) {
        const key = `${parsed.collectionId}`;
        if (!docsByCollection.has(key)) {
          docsByCollection.set(key, []);
        }
        docsByCollection.get(key)!.push(parsed.docId);
      }
    }

    const logParts: string[] = [];
    for (const [collection, docIds] of docsByCollection.entries()) {
      logParts.push(`${collection}/${docIds.join(',')}`);
    }
    const compactLog = logParts.join(' ');

    server.logger.log(
      'grpc',
      `BatchGetDocuments: ${compactLog || `${documents.length} docs`}`,
    );

    const responses: any[] = [];
    for (const docPath of documents) {
      const parsed = server.parseDocumentPath(docPath);

      if (!parsed) {
        server.logger.log(
          'grpc',
          `BatchGetDocuments response: MISSING - Invalid document path: ${docPath}`,
        );
        const now = new Date();
        responses.push({
          result: 'missing',
          missing: docPath,
          readTime: toTimestamp(now),
        });
        continue;
      }

      const document = server
        .getStorage()
        .getDocument(
          parsed.projectId,
          parsed.databaseId,
          parsed.collectionId,
          parsed.docId,
        );

      if (document) {
        const reconstructedFields = server.reconstructDocumentFields(document);
        const now = new Date();
        const defaultTimestamp = toTimestamp(now);
        const grpcFields = sanitizeGrpcFieldsForResponse(
          toGrpcFields(reconstructedFields),
        );
        const grpcDocument = {
          name: document.name,
          fields: grpcFields,
          createTime: document.createTime
            ? toTimestamp(new Date(document.createTime))
            : defaultTimestamp,
          updateTime: document.updateTime
            ? toTimestamp(new Date(document.updateTime))
            : defaultTimestamp,
        };
        responses.push({
          result: 'found',
          found: grpcDocument,
          readTime: defaultTimestamp,
        });
      } else {
        const now = new Date();
        responses.push({
          result: 'missing',
          missing: docPath,
          readTime: toTimestamp(now),
        });
      }
    }

    const writeNext = (index: number) => {
      if (index >= responses.length) {
        server.logger.log(
          'grpc',
          `BatchGetDocuments: ${compactLog || `${documents.length} docs`} ✓`,
        );
        call.end();
        return;
      }
      const res = responses[index];
      const outcome = res.found ? 'FOUND' : 'MISSING';
      const path = res.found ? res.found.name : res.missing;
      server.logger.log(
        'grpc',
        `BatchGetDocuments response[${index}]: ${outcome} ${path}`,
      );
      call.write(responses[index], (err?: Error) => {
        if (err) {
          server.logger.error(
            'grpc',
            `BatchGetDocuments write error: ${err.message}`,
          );
          call.destroy(err);
          return;
        }
        writeNext(index + 1);
      });
    };
    writeNext(0);
  } catch (error: unknown) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error';
    server.logger.error('grpc', `BatchGetDocuments error: ${errorMessage}`);
    const serviceError: grpc.ServiceError = {
      code: grpc.status.INTERNAL,
      message: errorMessage,
      name: 'InternalError',
      details: errorMessage,
      metadata: new grpc.Metadata(),
    };
    call.destroy(serviceError);
  }
}
