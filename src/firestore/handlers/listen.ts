/**
 * Listen gRPC handler (stub - not implemented)
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleListen(
  server: FirestoreServer,
  call: grpc.ServerDuplexStream<any, any>,
): void {
  server.logger.log(
    'grpc',
    'Listen RPC called (not implemented, closing stream)',
  );
  server.destroyStreamWithUnimplemented(
    call,
    'Listen (real-time) is not supported by this emulator',
  );
}
