/**
 * BeginTransaction gRPC handler.
 *
 * Generates a new transaction ID, tracks it in FirestoreStorage, and returns
 * it as bytes. The actual atomicity and rollback semantics live in the Commit
 * and Rollback handlers; this handler is just the coordination start.
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';

export function handleBeginTransaction(
  server: FirestoreServer,
  _call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    const txnId = server.getStorage().createTransaction();
    server.logger.log('grpc', `[BeginTransaction] created txnId=${txnId}`);
    callback(null, { transaction: Buffer.from(txnId, 'utf8') });
  } catch (error: unknown) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error';
    callback({
      code: grpc.status.INTERNAL,
      message: errorMessage,
    });
  }
}
