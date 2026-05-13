/**
 * Regression tests for dotted-path updates whose intermediate segments
 * contain characters the Admin SDK escapes with backticks on the wire
 * (colons, dashes, spaces, literal dots).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore field path backtick escaping (e2e)', () => {
  let db: admin.firestore.Firestore;
  const FieldPath = admin.firestore.FieldPath;
  const FieldValue = admin.firestore.FieldValue;

  before(function () {
    db = getFirestore();
  });

  it('string path: updates a key containing a dash', async function () {
    const ref = db.collection('fp_dash').doc('d' + Date.now());
    await ref.set({
      parent: { 'weird-key': 'old', sibling: 'keep' },
    });
    await ref.update({ 'parent.weird-key': 'new' });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      parent: { 'weird-key': 'new', sibling: 'keep' },
    });
  });

  it('string path: updates a key containing a colon', async function () {
    const ref = db.collection('fp_colon').doc('d' + Date.now());
    await ref.set({ root: { 'ns:id': 'old', sibling: 'keep' } });
    await ref.update({ 'root.ns:id': 'new' });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      root: { 'ns:id': 'new', sibling: 'keep' },
    });
  });

  it('FieldPath constructor: updates a key containing a space', async function () {
    const ref = db.collection('fp_space').doc('d' + Date.now());
    await ref.set({ root: { 'with space': 'old', sibling: 'keep' } });
    await ref.update(new FieldPath('root', 'with space'), 'new');
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      root: { 'with space': 'new', sibling: 'keep' },
    });
  });

  it('FieldPath constructor: updates a key containing a literal dot', async function () {
    const ref = db.collection('fp_dot').doc('d' + Date.now());
    await ref.set({ root: { 'with.dot': 'old', sibling: 'keep' } });
    await ref.update(new FieldPath('root', 'with.dot'), 'new');
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      root: { 'with.dot': 'new', sibling: 'keep' },
    });
  });

  it('FieldValue.delete() removes a backtick-escaped nested key', async function () {
    const ref = db.collection('fp_delete').doc('d' + Date.now());
    await ref.set({ parent: { 'weird-key': 'x', keep: 'y' } });
    await ref.update(new FieldPath('parent', 'weird-key'), FieldValue.delete());
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({ parent: { keep: 'y' } });
  });

  it('FieldValue.serverTimestamp() on a dashed nested key', async function () {
    const ref = db.collection('fp_ts').doc('d' + Date.now());
    await ref.set({ parent: { sibling: 'keep' } });
    await ref.update(
      new FieldPath('parent', 'last-seen'),
      FieldValue.serverTimestamp(),
    );
    const snap = await ref.get();
    const data = snap.data();
    expect(data?.parent?.sibling).to.equal('keep');
    expect(data?.parent?.['last-seen']).to.be.instanceOf(
      admin.firestore.Timestamp,
    );
  });
});
