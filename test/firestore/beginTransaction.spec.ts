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
});
