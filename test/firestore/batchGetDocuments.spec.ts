/**
 * Unit tests for the BatchGetDocuments gRPC handler (handlers/batchGetDocuments.ts).
 * Calls handleBatchGetDocuments directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleBatchGetDocuments } from '../../src/firestore/handlers/batchGetDocuments';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function batchGetDatabase(projectId: string, databaseId: string): string {
  return `projects/${projectId}/databases/${databaseId}`;
}

describe('Firestore BatchGetDocuments (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  function createMockCall(): {
    call: grpc.ServerWritableStream<any, any>;
    chunks: any[];
    endCalled: boolean;
    destroyCalled: boolean;
    destroyError: Error | null;
  } {
    const chunks: any[] = [];
    let endCalled = false;
    let destroyCalled = false;
    let destroyError: Error | null = null;
    const call = {
      request: { database: '', documents: [] as string[] },
      write: (chunk: any, callback: (err?: Error) => void) => {
        chunks.push(chunk);
        callback();
      },
      end: () => {
        endCalled = true;
      },
      destroy: (err?: Error) => {
        destroyCalled = true;
        destroyError = err ?? null;
      },
      logger: undefined,
    } as unknown as grpc.ServerWritableStream<any, any>;
    return {
      call,
      chunks: chunks,
      get endCalled() {
        return endCalled;
      },
      get destroyCalled() {
        return destroyCalled;
      },
      get destroyError() {
        return destroyError;
      },
    };
  }

  describe('handleBatchGetDocuments (direct)', () => {
    it('invalid database path: destroy with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        database: 'invalid-database',
        documents: [],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.destroyCalled).to.be.true;
          expect(mock.destroyError).to.exist;
          expect((mock.destroyError as grpc.ServiceError)?.code).to.equal(
            grpc.status.INVALID_ARGUMENT,
          );
          expect(mock.chunks).to.have.length(0);
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });

    it('empty documents: end without writing', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        database: batchGetDatabase(projectId, databaseId),
        documents: [],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.chunks).to.have.length(0);
          expect(mock.endCalled).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });

    it('one document found: writes one found then end', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'batch_get_coll';
      const docId = 'd1';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionId, docId, {
        name: path,
        fields: { title: { stringValue: 'Batch' } },
        createTime: now,
        updateTime: now,
      });

      const mock = createMockCall();
      mock.call.request = {
        database: batchGetDatabase(projectId, databaseId),
        documents: [path],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.chunks).to.have.length(1);
          expect(mock.chunks[0].result).to.equal('found');
          expect(mock.chunks[0].found?.name).to.equal(path);
          expect(mock.chunks[0].found?.fields?.title?.stringValue).to.equal(
            'Batch',
          );
          expect(mock.endCalled).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });

    it('one document missing: writes one missing then end', function (done) {
      const server = getFirestoreServer();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'batch_missing_coll',
        'no-doc',
      );
      const mock = createMockCall();
      mock.call.request = {
        database: batchGetDatabase(projectId, databaseId),
        documents: [path],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.chunks).to.have.length(1);
          expect(mock.chunks[0].result).to.equal('missing');
          expect(mock.chunks[0].missing).to.equal(path);
          expect(mock.endCalled).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });

    it('invalid document path in list: writes missing for that path', function (done) {
      const server = getFirestoreServer();
      const validPath = buildDocumentPath(
        projectId,
        databaseId,
        'batch_mixed_coll',
        'd1',
      );
      const storage = server.getStorage();
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, 'batch_mixed_coll', 'd1', {
        name: validPath,
        fields: {},
        createTime: now,
        updateTime: now,
      });

      const mock = createMockCall();
      mock.call.request = {
        database: batchGetDatabase(projectId, databaseId),
        documents: [validPath, 'invalid-path', validPath],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.chunks).to.have.length(3);
          expect(mock.chunks[0].result).to.equal('found');
          expect(mock.chunks[1].result).to.equal('missing');
          expect(mock.chunks[1].missing).to.equal('invalid-path');
          expect(mock.chunks[2].result).to.equal('found');
          expect(mock.endCalled).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });

    it('mixed found and missing: writes in order then end', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path1 = buildDocumentPath(
        projectId,
        databaseId,
        'batch_mixed2_coll',
        'a',
      );
      const path2 = buildDocumentPath(
        projectId,
        databaseId,
        'batch_mixed2_coll',
        'nonexistent',
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, 'batch_mixed2_coll', 'a', {
        name: path1,
        fields: { x: { integerValue: '1' } },
        createTime: now,
        updateTime: now,
      });

      const mock = createMockCall();
      mock.call.request = {
        database: batchGetDatabase(projectId, databaseId),
        documents: [path1, path2],
      };
      handleBatchGetDocuments(server, mock.call);
      setImmediate(() => {
        try {
          expect(mock.chunks).to.have.length(2);
          expect(mock.chunks[0].result).to.equal('found');
          expect(mock.chunks[0].found?.name).to.equal(path1);
          expect(mock.chunks[1].result).to.equal('missing');
          expect(mock.chunks[1].missing).to.equal(path2);
          expect(mock.endCalled).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });
  });
});
