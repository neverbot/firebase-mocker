/**
 * E2E tests for Write: the Write RPC (bidirectional stream) is not implemented.
 * Normal writes (set, update, delete) use Commit and work. This file documents
 * that the emulator works for writes via Commit; the Write stream is a separate stub.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore Write (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('writes via Commit (not Write RPC)', () => {
    it('set() and get() succeed; Write streaming RPC is unimplemented', async function () {
      const ref = db.collection('write_e2e').doc('d1');
      await ref.set({ value: 1 });
      const snap = await ref.get();
      expect(snap.exists).to.be.true;
      expect(snap.data()?.value).to.equal(1);
    });
  });
});
