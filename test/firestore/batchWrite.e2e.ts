/**
 * E2E tests for BatchWrite gRPC method via Admin SDK's BulkWriter and
 * firestore().recursiveDelete().
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore BatchWrite (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('BulkWriter', () => {
    it('creates many documents in parallel', async function () {
      const writer = db.bulkWriter();
      const col = db.collection('bw_create_' + Date.now());
      const count = 20;
      for (let i = 0; i < count; i++) {
        writer.set(col.doc('d' + i), { i });
      }
      await writer.close();

      const snap = await col.get();
      expect(snap.size).to.equal(count);
    });

    it('deletes many documents in parallel', async function () {
      const col = db.collection('bw_delete_' + Date.now());
      const setup = db.batch();
      for (let i = 0; i < 10; i++) {
        setup.set(col.doc('d' + i), { i });
      }
      await setup.commit();

      const writer = db.bulkWriter();
      for (let i = 0; i < 10; i++) {
        writer.delete(col.doc('d' + i));
      }
      await writer.close();

      const snap = await col.get();
      expect(snap.size).to.equal(0);
    });

    it('mixes set and delete in one BulkWriter', async function () {
      const col = db.collection('bw_mixed_' + Date.now());
      await col.doc('keep').set({ v: 1 });
      await col.doc('removeme').set({ v: 2 });

      const writer = db.bulkWriter();
      writer.set(col.doc('added'), { v: 3 });
      writer.delete(col.doc('removeme'));
      writer.update(col.doc('keep'), { v: 99 });
      await writer.close();

      const all = await col.get();
      const data = new Map(all.docs.map((d) => [d.id, d.data()]));
      expect(data.get('keep')).to.deep.equal({ v: 99 });
      expect(data.get('added')).to.deep.equal({ v: 3 });
      expect(data.has('removeme')).to.equal(false);
    });
  });

  // firestore.recursiveDelete() relies on "kindless all descendants" queries
  // (filtering by __name__ across subcollections) which RunQuery does not yet
  // implement. BulkWriter itself works — the gap is in the descendant query.
  describe.skip('recursiveDelete (requires kindless queries — not implemented)', () => {
    it('deletes a tree of docs and subcollection docs (9 total)', async function () {
      const root = db.collection('rd_root_' + Date.now());

      for (let i = 0; i < 3; i++) {
        const parent = root.doc('p' + i);
        await parent.set({ idx: i });
        for (let j = 0; j < 2; j++) {
          await parent
            .collection('child')
            .doc('c' + j)
            .set({ pi: i, cj: j });
        }
      }

      const before = await root.get();
      expect(before.size).to.equal(3);
      const childBefore = await root.doc('p0').collection('child').get();
      expect(childBefore.size).to.equal(2);

      await db.recursiveDelete(root);

      const afterRoot = await root.get();
      expect(afterRoot.size).to.equal(0);
      for (let i = 0; i < 3; i++) {
        const childAfter = await root
          .doc('p' + i)
          .collection('child')
          .get();
        expect(childAfter.size).to.equal(0);
      }
    });
  });
});
