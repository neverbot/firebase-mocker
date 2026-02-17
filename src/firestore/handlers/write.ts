/**
 * Write gRPC handler (stub - not implemented)
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleWrite(
  server: FirestoreServer,
  call: grpc.ServerDuplexStream<any, any>,
): void {
  server.logger.log(
    'grpc',
    'Write RPC called (not implemented, closing stream)',
  );
  server.destroyStreamWithUnimplemented(
    call,
    'Write (streaming) is not supported by this emulator',
  );
}
