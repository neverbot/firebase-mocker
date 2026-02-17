/**
 * Unit tests for the Listen gRPC handler (handlers/listen.ts).
 * Listen is a stub that closes the stream with UNIMPLEMENTED.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleListen } from '../../src/firestore/handlers/listen';
import { getFirestoreServer } from '../_setup';

describe('Firestore handler: Listen (unit)', () => {
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

  describe('handleListen (direct)', () => {
    it('calls destroyStreamWithUnimplemented and closes stream with UNIMPLEMENTED', function () {
      const server = getFirestoreServer();
      const mock = createMockDuplexCall();
      handleListen(server, mock.call);
      expect(mock.destroyCalled).to.be.true;
      expect(mock.destroyError).to.exist;
      expect((mock.destroyError as grpc.ServiceError)?.code).to.equal(
        grpc.status.UNIMPLEMENTED,
      );
      expect(mock.destroyError?.message).to.equal(
        'Listen (real-time) is not supported by this emulator',
      );
    });
  });
});
