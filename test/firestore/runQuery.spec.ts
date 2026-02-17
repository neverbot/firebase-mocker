/**
 * Unit tests for the RunQuery gRPC handler (handlers/runQuery.ts).
 * Calls handleRunQuery directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleRunQuery } from '../../src/firestore/handlers/runQuery';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function runQueryParent(projectId: string, databaseId: string): string {
  return `projects/${projectId}/databases/${databaseId}/documents`;
}

describe('Firestore RunQuery (unit)', () => {
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
      request: {},
      write: (chunk: any) => {
        chunks.push(chunk);
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
      chunks,
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

  function waitForRunQuery(done: (err?: Error) => void, assert: () => void) {
    setImmediate(() => {
      setImmediate(() => {
        try {
          assert();
          done();
        } catch (e) {
          done(e as Error);
        }
      });
    });
  }

  describe('handleRunQuery (direct)', () => {
    it('empty collection: writes one response with readTime and end', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: runQueryParent(projectId, databaseId),
        structured_query: {
          from: [{ collectionId: 'run_query_empty_coll' }],
        },
      };
      handleRunQuery(server, mock.call);
      waitForRunQuery(done, () => {
        expect(mock.chunks).to.have.length(1);
        expect(mock.chunks[0].readTime).to.exist;
        expect(mock.chunks[0].skippedResults).to.equal(0);
        expect(mock.chunks[0].document).to.be.undefined;
        expect(mock.endCalled).to.be.true;
      });
    });

    it('collection with documents: writes one response per document', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'run_query_docs_coll';
      const path1 = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        'd1',
      );
      const path2 = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        'd2',
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionId, 'd1', {
        name: path1,
        fields: { x: { stringValue: 'a' } },
        createTime: now,
        updateTime: now,
      });
      storage.setDocument(projectId, databaseId, collectionId, 'd2', {
        name: path2,
        fields: { x: { stringValue: 'b' } },
        createTime: now,
        updateTime: now,
      });

      const mock = createMockCall();
      mock.call.request = {
        parent: runQueryParent(projectId, databaseId),
        structured_query: {
          from: [{ collectionId }],
        },
      };
      handleRunQuery(server, mock.call);
      waitForRunQuery(done, () => {
        expect(mock.chunks).to.have.length(2);
        expect(mock.chunks[0].document).to.exist;
        expect(mock.chunks[0].document.name).to.include(collectionId);
        expect(mock.chunks[0].document.fields?.x?.stringValue).to.equal('a');
        expect(mock.chunks[1].document.fields?.x?.stringValue).to.equal('b');
        expect(mock.endCalled).to.be.true;
      });
    });

    it('from with collection_id (snake_case)', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: runQueryParent(projectId, databaseId),
        structured_query: {
          from: [{ collection_id: 'run_query_snake_coll' }],
        },
      };
      handleRunQuery(server, mock.call);
      waitForRunQuery(done, () => {
        expect(mock.chunks).to.have.length(1);
        expect(mock.chunks[0].readTime).to.exist;
        expect(mock.endCalled).to.be.true;
      });
    });

    it('request serialization failure: destroy with INTERNAL', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      // BigInt cannot be serialized by JSON.stringify, triggers catch in handler
      mock.call.request = { parent: 'x', foo: BigInt(1) };
      handleRunQuery(server, mock.call);
      waitForRunQuery(done, () => {
        expect(mock.destroyCalled).to.be.true;
        expect((mock.destroyError as grpc.ServiceError)?.code).to.equal(
          grpc.status.INTERNAL,
        );
        expect((mock.destroyError as grpc.ServiceError)?.message).to.include(
          'serialization',
        );
      });
    });
  });
});
