/**
 * Unit tests for the BeginTransaction handler.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleBeginTransaction } from '../../src/firestore/handlers/beginTransaction';
import { getFirestoreServer } from '../_setup';

describe('Firestore BeginTransaction (unit)', () => {
  it('callback receives {transaction: Buffer} and the txnId is in storage', function (done) {
    const server = getFirestoreServer();
    const call = {
      request: {
        database: 'projects/test-project/databases/(default)',
      },
    } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(err).to.be.null;
        const response = value as { transaction?: Buffer };
        expect(response.transaction).to.exist;
        expect(Buffer.isBuffer(response.transaction)).to.be.true;
        const txnId = response.transaction!.toString('utf8');
        expect(server.getStorage().hasTransaction(txnId)).to.be.true;
        // cleanup
        server.getStorage().endTransaction(txnId);
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleBeginTransaction(server, call, callback);
  });

  it('returns INTERNAL when createTransaction throws an Error', function (done) {
    const fakeServer = {
      getStorage: () => ({
        createTransaction: () => {
          throw new Error('boom');
        },
      }),
      logger: { log: () => {} },
    } as unknown as Parameters<typeof handleBeginTransaction>[0];

    const call = { request: {} } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(value).to.be.undefined;
        expect(err).to.exist;
        const e = err as grpc.ServiceError;
        expect(e.code).to.equal(grpc.status.INTERNAL);
        expect(e.message).to.equal('boom');
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleBeginTransaction(fakeServer, call, callback);
  });

  it('returns INTERNAL with "Unknown error" when createTransaction throws a non-Error', function (done) {
    const fakeServer = {
      getStorage: () => ({
        createTransaction: () => {
          // eslint-disable-next-line @typescript-eslint/only-throw-error
          throw 'string-thrown';
        },
      }),
      logger: { log: () => {} },
    } as unknown as Parameters<typeof handleBeginTransaction>[0];

    const call = { request: {} } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(value).to.be.undefined;
        expect(err).to.exist;
        const e = err as grpc.ServiceError;
        expect(e.code).to.equal(grpc.status.INTERNAL);
        expect(e.message).to.equal('Unknown error');
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleBeginTransaction(fakeServer, call, callback);
  });
});
