/**
 * E2E tests for BatchGetDocuments: use the firebase-admin client from _setup
 * to perform getAll() or parallel get() against the emulator.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore BatchGetDocuments (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('getAll', () => {
    it('getAll returns snapshots for existing documents', async function () {
      const collectionName = 'batch_get_e2e_coll';
      const doc1 = db.collection(collectionName).doc('d1');
      const doc2 = db.collection(collectionName).doc('d2');

      await doc1.set({ name: 'First' });
      await doc2.set({ name: 'Second' });

      const snapshots = await db.getAll(doc1, doc2);

      expect(snapshots).to.have.length(2);
      expect(snapshots[0].exists).to.be.true;
      expect(snapshots[0].id).to.equal('d1');
      expect(snapshots[0].data()?.name).to.equal('First');
      expect(snapshots[1].exists).to.be.true;
      expect(snapshots[1].id).to.equal('d2');
      expect(snapshots[1].data()?.name).to.equal('Second');
    });

    it('getAll includes missing documents as exists false', async function () {
      const collectionName = 'batch_get_e2e_mixed';
      const docExisting = db.collection(collectionName).doc('existing');
      const docMissing = db.collection(collectionName).doc('missing');

      await docExisting.set({ value: 1 });

      const snapshots = await db.getAll(docExisting, docMissing);

      expect(snapshots).to.have.length(2);
      expect(snapshots[0].exists).to.be.true;
      expect(snapshots[0].data()?.value).to.equal(1);
      expect(snapshots[1].exists).to.be.false;
      expect(snapshots[1].id).to.equal('missing');
    });
  });

  describe('parallel get', () => {
    it('Promise.all([...get()]) returns correct data', async function () {
      const collectionName = 'batch_get_e2e_parallel';
      const refs = [
        db.collection(collectionName).doc('a'),
        db.collection(collectionName).doc('b'),
      ];
      await refs[0].set({ x: 1 });
      await refs[1].set({ x: 2 });

      const snaps = await Promise.all(refs.map(async (r) => r.get()));

      expect(snaps[0].exists).to.be.true;
      expect(snaps[0].data()?.x).to.equal(1);
      expect(snaps[1].exists).to.be.true;
      expect(snaps[1].data()?.x).to.equal(2);
    });
  });
});
