/**
 * Unit tests for the Write gRPC handler (handlers/write.ts).
 * Write is a stub that closes the stream with UNIMPLEMENTED.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleWrite } from '../../src/firestore/handlers/write';
import { getFirestoreServer } from '../_setup';

describe('Firestore Write (unit)', () => {
  function createMockDuplexCall(): {
    call: grpc.ServerDuplexStream<any, any>;
    destroyCalled: boolean;
    destroyError: Error | null;
  } {
    let destroyCalled = false;
    let destroyError: Error | null = null;
    const call = {
      destroy: (err?: Error) => {
        destroyCalled = true;
        destroyError = err ?? null;
      },
      on: () => call,
      write: () => {},
      end: () => {},
      logger: undefined,
    } as unknown as grpc.ServerDuplexStream<any, any>;
    return {
      call,
      get destroyCalled() {
        return destroyCalled;
      },
      get destroyError() {
        return destroyError;
      },
    };
  }

  describe('handleWrite (direct)', () => {
    it('calls destroyStreamWithUnimplemented and closes stream with UNIMPLEMENTED', function () {
      const server = getFirestoreServer();
      const mock = createMockDuplexCall();
      handleWrite(server, mock.call);
      expect(mock.destroyCalled).to.be.true;
      expect(mock.destroyError).to.exist;
      expect((mock.destroyError as grpc.ServiceError)?.code).to.equal(
        grpc.status.UNIMPLEMENTED,
      );
      expect(mock.destroyError?.message).to.equal(
        'Write (streaming) is not supported by this emulator',
      );
    });
  });
});
