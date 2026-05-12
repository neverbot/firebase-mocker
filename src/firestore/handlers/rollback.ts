/**
 * Rollback gRPC handler.
 *
 * Removes the transaction ID from storage and returns an empty response.
 * In Level 1 semantics, this is a no-op cleanup since writes are never
 * buffered server-side (the SDK accumulates them and sends them in Commit
 * only on success). Rollback is tolerant: unknown or missing transaction
 * IDs do not raise errors.
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleRollback(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const txnBytes = call.request.transaction;
    if (txnBytes) {
      const txnId = Buffer.isBuffer(txnBytes)
        ? txnBytes.toString('utf8')
        : String(txnBytes);
      const existed = server.getStorage().endTransaction(txnId);
      server.logger.log('grpc', `[Rollback] txnId=${txnId} existed=${existed}`);
    } else {
      server.logger.log('grpc', '[Rollback] no transaction field in request');
    }
    callback(null, {});
  } catch (error: unknown) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error';
    callback({
      code: grpc.status.INTERNAL,
      message: errorMessage,
    });
  }
}
