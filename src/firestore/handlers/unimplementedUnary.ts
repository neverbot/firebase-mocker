/**
 * Stub for unimplemented unary RPCs (BatchWrite, BeginTransaction, Rollback)
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleUnimplementedUnary(
  server: FirestoreServer,
  _call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
  rpcName: string,
): void {
  try {
    server.emitUnimplementedWarning(rpcName);
  } catch (err) {
    callback(err as grpc.ServiceError, null);
    return;
  }
  callback(
    {
      code: grpc.status.UNIMPLEMENTED,
      message: `${rpcName} is not supported by this emulator`,
      details: `${rpcName} is not supported by this emulator`,
    } as grpc.ServiceError,
    null,
  );
}
