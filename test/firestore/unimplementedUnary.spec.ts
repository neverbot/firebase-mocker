/**
 * Unit tests for the unimplemented-unary stub (handlers/unimplementedUnary.ts).
 *
 * These tests do not cover real feature code. They assert the behaviour of the
 * emulator when Firestore RPCs that are not yet supported are called: the stub
 * emits a warning and responds with UNIMPLEMENTED.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleUnimplementedUnary } from '../../src/firestore/handlers/unimplementedUnary';
import { getFirestoreServer } from '../_setup';

describe('Firestore unimplementedUnary (unit)', () => {
  function createMockCall(): grpc.ServerUnaryCall<any, any> {
    return {
      request: {},
    } as grpc.ServerUnaryCall<any, any>;
  }

  describe('handleUnimplementedUnary (direct)', () => {
    ['BatchWrite', 'BeginTransaction', 'Rollback'].forEach((rpcName) => {
      it(`${rpcName}: callback with UNIMPLEMENTED and "not supported" message`, function (done) {
        const server = getFirestoreServer();
        const call = createMockCall();
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.exist;
            expect((err as grpc.ServiceError)?.code).to.equal(
              grpc.status.UNIMPLEMENTED,
            );
            expect((err as { message?: string })?.message).to.equal(
              `${rpcName} is not supported by this emulator`,
            );
            expect(value === null || value === undefined).to.be.true;
            done();
          } catch (e) {
            done(e as Error);
          }
        };
        handleUnimplementedUnary(server, call, callback, rpcName);
      });
    });

    it('when emitUnimplementedWarning throws: callback receives that error', function (done) {
      const server = getFirestoreServer();
      const thrown = new Error('config threw') as grpc.ServiceError;
      server.emitUnimplementedWarning = () => {
        throw thrown;
      };
      const call = createMockCall();
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.equal(thrown);
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleUnimplementedUnary(server, call, callback, 'BeginTransaction');
    });
  });
});
