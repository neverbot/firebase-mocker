/**
 * Unit tests for the RunAggregationQuery gRPC handler (handlers/runAggregationQuery.ts).
 * Calls handleRunAggregationQuery directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleRunAggregationQuery } from '../../src/firestore/handlers/runAggregationQuery';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function aggregationParent(projectId: string, databaseId: string): string {
  return `projects/${projectId}/databases/${databaseId}/documents`;
}

describe('Firestore handler: RunAggregationQuery (unit)', () => {
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

  function waitForHandler(done: (err?: Error) => void, assert: () => void) {
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

  describe('handleRunAggregationQuery (direct)', () => {
    it('empty collection: writes one response with count 0 and end', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: aggregationParent(projectId, databaseId),
        structured_aggregation_query: {
          structured_query: {
            from: [{ collectionId: 'agg_empty_coll' }],
          },
        },
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
        expect(mock.chunks).to.have.length(1);
        expect(
          mock.chunks[0].result?.aggregateFields?.count?.integerValue,
        ).to.equal('0');
        expect(mock.chunks[0].readTime).to.exist;
        expect(mock.endCalled).to.be.true;
      });
    });

    it('collection with documents: count equals document count', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'agg_docs_coll';
      const now = new Date().toISOString();
      for (let i = 0; i < 3; i++) {
        const docId = `d${i}`;
        const path = buildDocumentPath(
          projectId,
          databaseId,
          collectionId,
          docId,
        );
        storage.setDocument(projectId, databaseId, collectionId, docId, {
          name: path,
          fields: { n: { integerValue: String(i) } },
          createTime: now,
          updateTime: now,
        });
      }

      const mock = createMockCall();
      mock.call.request = {
        parent: aggregationParent(projectId, databaseId),
        structured_aggregation_query: {
          structured_query: { from: [{ collectionId }] },
        },
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
        expect(mock.chunks).to.have.length(1);
        expect(
          mock.chunks[0].result?.aggregateFields?.count?.integerValue,
        ).to.equal('3');
        expect(mock.endCalled).to.be.true;
      });
    });

    it('from with collection_id (snake_case)', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: aggregationParent(projectId, databaseId),
        structured_aggregation_query: {
          structured_query: {
            from: [{ collection_id: 'agg_snake_coll' }],
          },
        },
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
        expect(
          mock.chunks[0].result?.aggregateFields?.count?.integerValue,
        ).to.equal('0');
        expect(mock.endCalled).to.be.true;
      });
    });

    it('explicit count aggregation with alias', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'agg_alias_coll';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        'one',
      );
      storage.setDocument(projectId, databaseId, collectionId, 'one', {
        name: path,
        fields: {},
        createTime: new Date().toISOString(),
        updateTime: new Date().toISOString(),
      });

      const mock = createMockCall();
      mock.call.request = {
        parent: aggregationParent(projectId, databaseId),
        structured_aggregation_query: {
          structured_query: { from: [{ collectionId }] },
          aggregations: [{ count: {}, alias: 'total' }],
        },
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
        expect(
          mock.chunks[0].result?.aggregateFields?.total?.integerValue,
        ).to.equal('1');
        expect(mock.chunks[0].result?.aggregateFields?.count).to.be.undefined;
        expect(mock.endCalled).to.be.true;
      });
    });

    it('sum/avg return 0 when requested', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: aggregationParent(projectId, databaseId),
        structured_aggregation_query: {
          structured_query: { from: [{ collectionId: 'agg_sum_avg_coll' }] },
          aggregations: [
            { sum: { field: { fieldPath: 'x' } }, alias: 's' },
            { avg: { field: { fieldPath: 'x' } }, alias: 'a' },
          ],
        },
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
        expect(
          mock.chunks[0].result?.aggregateFields?.s?.integerValue,
        ).to.equal('0');
        expect(mock.chunks[0].result?.aggregateFields?.a?.doubleValue).to.equal(
          0,
        );
        expect(mock.endCalled).to.be.true;
      });
    });

    it('request serialization failure: destroy with INTERNAL', function (done) {
      const server = getFirestoreServer();
      const mock = createMockCall();
      mock.call.request = {
        parent: 'x',
        structured_aggregation_query: {},
        foo: BigInt(1),
      };
      handleRunAggregationQuery(server, mock.call);
      waitForHandler(done, () => {
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
