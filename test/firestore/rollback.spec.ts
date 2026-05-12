/**
 * Unit tests for the Rollback handler.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleRollback } from '../../src/firestore/handlers/rollback';
import { getFirestoreServer } from '../_setup';

describe('Firestore Rollback (unit)', () => {
  it('removes the transaction ID from storage and calls back with {}', function (done) {
    const server = getFirestoreServer();
    const txnId = server.getStorage().createTransaction();

    const call = {
      request: {
        database: 'projects/test-project/databases/(default)',
        transaction: Buffer.from(txnId, 'utf8'),
      },
    } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(err).to.be.null;
        expect(value).to.deep.equal({});
        expect(server.getStorage().hasTransaction(txnId)).to.be.false;
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleRollback(server, call, callback);
  });

  it('returns success even for unknown transaction ID (tolerant)', function (done) {
    const server = getFirestoreServer();
    const call = {
      request: {
        database: 'projects/test-project/databases/(default)',
        transaction: Buffer.from('does-not-exist', 'utf8'),
      },
    } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(err).to.be.null;
        expect(value).to.deep.equal({});
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleRollback(server, call, callback);
  });

  it('returns success when request is missing transaction field', function (done) {
    const server = getFirestoreServer();
    const call = {
      request: { database: 'projects/test-project/databases/(default)' },
    } as grpc.ServerUnaryCall<any, any>;

    const callback: grpc.sendUnaryData<any> = (err, value) => {
      try {
        expect(err).to.be.null;
        expect(value).to.deep.equal({});
        done();
      } catch (assertionErr) {
        done(assertionErr);
      }
    };

    handleRollback(server, call, callback);
  });
});
