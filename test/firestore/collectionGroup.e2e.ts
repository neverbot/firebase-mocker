/**
 * E2E tests for collectionGroup queries (RunQuery with allDescendants).
 */

import { expect } from 'chai';
import { Firestore } from 'firebase-admin/firestore';
import { getFirestore } from '../_setup';

describe('Firestore collectionGroup (e2e)', () => {
  let db: Firestore;

  before(function () {
    db = getFirestore();
  });

  it('returns docs whose leaf collection matches across the database', async function () {
    const stamp = Date.now();
    const root = `cg_root_${stamp}`;
    const leaf = `cg_leaf_${stamp}`;
    await db
      .collection(root)
      .doc('p1')
      .collection(leaf)
      .doc('c1')
      .set({ tag: 'a' });
    await db
      .collection(root)
      .doc('p1')
      .collection(leaf)
      .doc('c2')
      .set({ tag: 'b' });
    await db
      .collection(root)
      .doc('p2')
      .collection(leaf)
      .doc('c3')
      .set({ tag: 'c' });
    await db
      .collection(root)
      .doc('p3')
      .collection(`cg_other_${stamp}`)
      .doc('o1')
      .set({ tag: 'd' });

    const snap = await db.collectionGroup(leaf).get();
    const tags = snap.docs.map((d) => d.data().tag).sort();
    expect(tags).to.deep.equal(['a', 'b', 'c']);
  });

  it('filters with where on collectionGroup', async function () {
    const stamp = Date.now();
    const root = `cg_filter_${stamp}`;
    const leaf = `cg_items_${stamp}`;
    await db
      .collection(root)
      .doc('p1')
      .collection(leaf)
      .doc('i1')
      .set({ active: true });
    await db
      .collection(root)
      .doc('p1')
      .collection(leaf)
      .doc('i2')
      .set({ active: false });
    await db
      .collection(root)
      .doc('p2')
      .collection(leaf)
      .doc('i3')
      .set({ active: true });

    const snap = await db
      .collectionGroup(leaf)
      .where('active', '==', true)
      .get();
    expect(snap.size).to.equal(2);
  });
});
