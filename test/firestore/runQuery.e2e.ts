/**
 * E2E tests for RunQuery: use the firebase-admin client from _setup
 * to perform collection.get() and collection.where().get() against the emulator.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore RunQuery (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('collection.get()', () => {
    it('returns snapshot with docs for collection with documents', async function () {
      const collectionName = 'run_query_e2e_coll';
      const col = db.collection(collectionName);
      await col.doc('a').set({ name: 'A' });
      await col.doc('b').set({ name: 'B' });

      const snapshot = await col.get();

      expect(snapshot.empty).to.be.false;
      expect(snapshot.size).to.equal(2);
      const ids = snapshot.docs.map((d) => d.id).sort();
      expect(ids).to.deep.equal(['a', 'b']);
      expect(snapshot.docs.find((d) => d.id === 'a')?.data()?.name).to.equal(
        'A',
      );
    });

    it('returns empty snapshot for empty collection', async function () {
      const col = db.collection('run_query_e2e_empty');
      const snapshot = await col.get();
      expect(snapshot.empty).to.be.true;
      expect(snapshot.size).to.equal(0);
    });
  });

  describe('collection.where().get()', () => {
    it('filters by field value', async function () {
      const collectionName = 'run_query_e2e_where';
      const col = db.collection(collectionName);
      await col.doc('1').set({ status: 'active', n: 1 });
      await col.doc('2').set({ status: 'inactive', n: 2 });
      await col.doc('3').set({ status: 'active', n: 3 });

      const snapshot = await col.where('status', '==', 'active').get();

      expect(snapshot.size).to.equal(2);
      const ids = snapshot.docs.map((d) => d.id).sort();
      expect(ids).to.deep.equal(['1', '3']);
    });
  });

  describe('subcollection query', () => {
    it('query on subcollection returns documents', async function () {
      const parentRef = db.collection('run_query_e2e_parents').doc('p1');
      await parentRef.set({ name: 'Parent' });
      const sub = parentRef.collection('items');
      await sub.doc('i1').set({ name: 'Item 1' });
      await sub.doc('i2').set({ name: 'Item 2' });

      const snapshot = await sub.get();

      expect(snapshot.size).to.equal(2);
      expect(snapshot.docs.map((d) => d.data().name).sort()).to.deep.equal([
        'Item 1',
        'Item 2',
      ]);
    });
  });
});
