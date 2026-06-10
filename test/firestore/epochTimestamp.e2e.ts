/**
 * Regression test: `new Date(0)` (Unix epoch) must round-trip unchanged.
 * proto3 elides integer fields whose value is 0, so an epoch Timestamp
 * arrives at the server as `{}`. The emulator previously fell back to
 * `new Date()` (the current clock), corrupting epoch values to NOW.
 */

import { expect } from 'chai';
import { Firestore, Timestamp } from 'firebase-admin/firestore';
import { getFirestore } from '../_setup';

describe('Firestore epoch Timestamp round-trip (e2e)', () => {
  let db: Firestore;

  before(function () {
    db = getFirestore();
  });

  it('new Date(0) survives a write+read cycle', async function () {
    const ref = db.collection('epoch').doc('d' + Date.now());
    await ref.set({ ts: new Date(0) });
    const snap = await ref.get();
    const ts = snap.data()?.ts as Timestamp;
    expect(ts).to.be.instanceOf(Timestamp);
    expect(ts.toMillis()).to.equal(0);
    expect(ts.toDate().toISOString()).to.equal('1970-01-01T00:00:00.000Z');
  });

  it('Timestamp.fromMillis(0) survives a write+read cycle', async function () {
    const ref = db.collection('epoch').doc('d' + Date.now());
    await ref.set({ ts: Timestamp.fromMillis(0) });
    const snap = await ref.get();
    const ts = snap.data()?.ts as Timestamp;
    expect(ts.toMillis()).to.equal(0);
  });

  it('does not corrupt non-epoch dates', async function () {
    const ref = db.collection('epoch').doc('d' + Date.now());
    const original = new Date('2020-01-15T12:34:56.789Z');
    await ref.set({ ts: original });
    const snap = await ref.get();
    const ts = snap.data()?.ts as Timestamp;
    expect(ts.toMillis()).to.equal(original.getTime());
  });

  it('preserves epoch when used as a sentinel for unpublished docs', async function () {
    const ref = db.collection('epoch').doc('d' + Date.now());
    await ref.set({ status: 'draft', publishedAt: new Date(0) });
    const snap = await ref.get();
    const publishedAt = snap.data()?.publishedAt as Timestamp;
    expect(publishedAt.toMillis()).to.equal(0);
  });
});
