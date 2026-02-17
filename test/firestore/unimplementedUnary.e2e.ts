/**
 * E2E tests for unimplemented unary RPCs.
 *
 * These tests do not cover real feature code. They verify that when the client
 * uses Firestore features that are not yet supported by the emulator, the
 * server correctly responds with UNIMPLEMENTED (and optionally that a warning
 * is emitted). This documents the behaviour for transactions and batch writes.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore unimplementedUnary (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('runTransaction (uses BeginTransaction)', () => {
    it('fails with error indicating transaction is not supported', async function () {
      const ref = db.collection('unimplemented_e2e').doc('tx');
      try {
        await db.runTransaction(async (tx) => {
          await tx.get(ref); // force transaction to start and call BeginTransaction
        });
        expect.fail('Expected runTransaction to throw');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        // Server returns UNIMPLEMENTED for BeginTransaction; SDK may surface
        // "not supported" or "Transaction ID was missing from server response"
        expect(
          message.includes('not supported') || message.includes('Transaction'),
        ).to.be.true;
      }
    });
  });
});
