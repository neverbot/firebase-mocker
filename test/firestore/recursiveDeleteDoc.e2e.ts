/**
 * Regression tests for `firestore.recursiveDelete(documentRef)`. Targets the
 * orchestration via RunQuery's kindless `allDescendants` mode when the ref
 * is a DocumentReference (no `__name__` range filter is sent; the scope is
 * the doc path in `request.parent`).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore recursiveDelete on documents (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  it('deletes a doc and its direct subcollection', async function () {
    const root = db.collection('rdd_simple_' + Date.now()).doc('p1');
    await root.set({ idx: 1 });
    await root.collection('child').doc('c1').set({ v: 'a' });
    await root.collection('child').doc('c2').set({ v: 'b' });

    await db.recursiveDelete(root);

    const after = await root.get();
    expect(after.exists).to.equal(false);
    const childAfter = await root.collection('child').get();
    expect(childAfter.size).to.equal(0);
  });

  it('deletes multi-level nested subcollections under a document', async function () {
    const root = db.collection('rdd_nested_' + Date.now()).doc('p1');
    await root.set({ idx: 1 });
    await root.collection('a').doc('a1').set({ v: 1 });
    await root
      .collection('a')
      .doc('a1')
      .collection('b')
      .doc('b1')
      .set({ v: 2 });
    await root
      .collection('a')
      .doc('a1')
      .collection('b')
      .doc('b1')
      .collection('c')
      .doc('c1')
      .set({ v: 3 });

    await db.recursiveDelete(root);

    const a = await root.collection('a').get();
    expect(a.size).to.equal(0);
    const b = await root.collection('a').doc('a1').collection('b').get();
    expect(b.size).to.equal(0);
    const c = await root
      .collection('a')
      .doc('a1')
      .collection('b')
      .doc('b1')
      .collection('c')
      .get();
    expect(c.size).to.equal(0);
  });

  it('deletes a doc with multiple sibling subcollections', async function () {
    const root = db.collection('rdd_multi_' + Date.now()).doc('p1');
    await root.set({ idx: 1 });
    await root.collection('items').doc('i1').set({ x: 1 });
    await root.collection('items').doc('i2').set({ x: 2 });
    await root.collection('tags').doc('t1').set({ y: 'a' });

    await db.recursiveDelete(root);

    expect((await root.get()).exists).to.equal(false);
    expect((await root.collection('items').get()).size).to.equal(0);
    expect((await root.collection('tags').get()).size).to.equal(0);
  });

  it('deletes a doc with no subcollections', async function () {
    const root = db.collection('rdd_empty_' + Date.now()).doc('p1');
    await root.set({ only: true });

    await db.recursiveDelete(root);

    expect((await root.get()).exists).to.equal(false);
  });

  it('still works for recursiveDelete on a collection (regression)', async function () {
    const stamp = Date.now();
    const col = db.collection('rdd_col_' + stamp);
    for (let i = 0; i < 3; i++) {
      const p = col.doc('p' + i);
      await p.set({ idx: i });
      await p.collection('child').doc('c0').set({ v: i });
    }

    await db.recursiveDelete(col);

    expect((await col.get()).size).to.equal(0);
    expect((await col.doc('p0').collection('child').get()).size).to.equal(0);
  });
});
