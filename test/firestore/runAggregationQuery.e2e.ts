/**
 * E2E tests for RunAggregationQuery: use firebase-admin client from _setup
 * to run count() aggregation queries against the emulator.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore RunAggregationQuery (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('collection.count().get()', () => {
    it('returns count 0 for empty collection', async function () {
      const col = db.collection('agg_e2e_empty');
      const snapshot = await col.count().get();
      expect(snapshot.data().count).to.equal(0);
    });

    it('returns document count for non-empty collection', async function () {
      const col = db.collection('agg_e2e_count');
      await col.doc('a').set({ x: 1 });
      await col.doc('b').set({ x: 2 });
      await col.doc('c').set({ x: 3 });

      const snapshot = await col.count().get();
      expect(snapshot.data().count).to.equal(3);
    });
  });

  describe('query.count().get() with filter', () => {
    it('returns count of matching documents', async function () {
      const col = db.collection('agg_e2e_where');
      await col.doc('1').set({ status: 'active' });
      await col.doc('2').set({ status: 'inactive' });
      await col.doc('3').set({ status: 'active' });

      const snapshot = await col.where('status', '==', 'active').count().get();
      expect(snapshot.data().count).to.equal(2);
    });
  });
});
