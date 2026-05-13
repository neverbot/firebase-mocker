/**
 * BatchWrite gRPC handler.
 *
 * Unlike Commit, BatchWrite applies each Write independently and returns a
 * per-Write status. The batch as a whole is NOT atomic — partial success is
 * allowed. We reuse `handleCommit` for each individual Write to keep the
 * write-application logic in one place.
 */

import * as grpc from '@grpc/grpc-js';
import type { FirestoreServer } from '../server';
import { handleCommit } from './commit';

interface BatchWriteItemOutcome {
  writeResult: any;
  status: { code: number; message: string };
}

function applySingleWrite(
  server: FirestoreServer,
  database: string,
  write: any,
): BatchWriteItemOutcome {
  const syntheticCall = {
    request: { database, writes: [write] },
  } as grpc.ServerUnaryCall<any, any>;

  let capturedErr: grpc.ServiceError | null = null;
  let capturedResponse: any = null;
  handleCommit(server, syntheticCall, (err, response) => {
    capturedErr = err as grpc.ServiceError | null;
    capturedResponse = response;
  });

  if (capturedErr) {
    const e = capturedErr as grpc.ServiceError;
    return {
      writeResult: {},
      status: {
        code: e.code ?? grpc.status.INTERNAL,
        message: e.message ?? 'BatchWrite item failed',
      },
    };
  }

  const results =
    capturedResponse?.write_results ?? capturedResponse?.writeResults ?? [];
  const raw = results[0] ?? {};
  const updateTime = raw.update_time ?? raw.updateTime;
  return {
    writeResult: updateTime ? { updateTime } : {},
    status: { code: 0, message: '' },
  };
}

export function handleBatchWrite(
  server: FirestoreServer,
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
): void {
  try {
    server.logger.log('grpc', '[BatchWrite] Called');
    const request = call.request;
    const database = request.database || '';
    const rawWrites = request.writes;
    const writes = Array.isArray(rawWrites)
      ? rawWrites
      : rawWrites && typeof rawWrites === 'object'
        ? Object.values(rawWrites)
        : [];

    server.logger.log(
      'grpc',
      `[BatchWrite] database=${database} writes=${writes.length}`,
    );

    const writeResults: any[] = [];
    const statuses: any[] = [];

    for (const write of writes) {
      const outcome = applySingleWrite(server, database, write);
      writeResults.push(outcome.writeResult);
      statuses.push(outcome.status);
    }

    callback(null, {
      writeResults,
      status: statuses,
    });
  } catch (error: unknown) {
    callback({
      code: grpc.status.INTERNAL,
      message: error instanceof Error ? error.message : 'Unknown error',
    });
  }
}
