/**
 * E2E tests for FieldValue transforms: increment, maximum, minimum,
 * arrayUnion, arrayRemove. Use firebase-admin client to verify the
 * SDK-encoded transforms are applied correctly by the Commit handler.
 */

import { expect } from 'chai';
import { Firestore, FieldValue } from 'firebase-admin/firestore';
import { getFirestore } from '../_setup';

describe('Firestore FieldValue transforms (e2e)', () => {
  let db: Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('increment', () => {
    it('increments existing integer field', async function () {
      const ref = db.collection('xform_inc').doc('d' + Date.now());
      await ref.set({ count: 5 });
      await ref.update({ count: FieldValue.increment(3) });
      const snap = await ref.get();
      expect(snap.data()?.count).to.equal(8);
    });

    it('treats missing field as 0', async function () {
      const ref = db.collection('xform_inc').doc('d' + Date.now());
      await ref.set({ other: 1 });
      await ref.update({ count: FieldValue.increment(7) });
      const snap = await ref.get();
      expect(snap.data()?.count).to.equal(7);
      expect(snap.data()?.other).to.equal(1);
    });

    it('produces double when increment is a float', async function () {
      const ref = db.collection('xform_inc').doc('d' + Date.now());
      await ref.set({ count: 1 });
      await ref.update({ count: FieldValue.increment(0.5) });
      const snap = await ref.get();
      expect(snap.data()?.count).to.equal(1.5);
    });

    it('supports negative increments', async function () {
      const ref = db.collection('xform_inc').doc('d' + Date.now());
      await ref.set({ count: 10 });
      await ref.update({ count: FieldValue.increment(-4) });
      const snap = await ref.get();
      expect(snap.data()?.count).to.equal(6);
    });

    it('works via set({merge: true})', async function () {
      const ref = db.collection('xform_inc').doc('d' + Date.now());
      await ref.set({ count: 2 });
      await ref.set({ count: FieldValue.increment(5) }, { merge: true });
      const snap = await ref.get();
      expect(snap.data()?.count).to.equal(7);
    });
  });

  describe('arrayUnion', () => {
    it('appends new elements without duplicates', async function () {
      const ref = db.collection('xform_arr').doc('d' + Date.now());
      await ref.set({ tags: ['a', 'b'] });
      await ref.update({ tags: FieldValue.arrayUnion('b', 'c', 'd') });
      const snap = await ref.get();
      expect(snap.data()?.tags).to.deep.equal(['a', 'b', 'c', 'd']);
    });

    it('creates array when field is missing', async function () {
      const ref = db.collection('xform_arr').doc('d' + Date.now());
      await ref.set({ name: 'x' });
      await ref.update({ tags: FieldValue.arrayUnion('a', 'b') });
      const snap = await ref.get();
      expect(snap.data()?.tags).to.deep.equal(['a', 'b']);
    });
  });

  describe('arrayRemove', () => {
    it('removes matching elements', async function () {
      const ref = db.collection('xform_arr').doc('d' + Date.now());
      await ref.set({ tags: ['a', 'b', 'c', 'b'] });
      await ref.update({ tags: FieldValue.arrayRemove('b') });
      const snap = await ref.get();
      expect(snap.data()?.tags).to.deep.equal(['a', 'c']);
    });

    it('is a no-op when field is missing', async function () {
      const ref = db.collection('xform_arr').doc('d' + Date.now());
      await ref.set({ name: 'x' });
      await ref.update({ tags: FieldValue.arrayRemove('a') });
      const snap = await ref.get();
      expect(snap.data()?.tags).to.deep.equal([]);
    });
  });
});
