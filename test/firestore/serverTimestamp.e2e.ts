/**
 * E2E verification that FieldValue.serverTimestamp() is replaced with a
 * concrete timestamp before being persisted, and that the emulator-clock
 * value echoed in the commit response (`commit_time`) matches the value
 * the Admin SDK reads back from storage.
 */

import { expect } from 'chai';
import { Firestore, FieldValue, Timestamp } from 'firebase-admin/firestore';
import { getFirestore } from '../_setup';

describe('Firestore serverTimestamp normalization (e2e)', () => {
  let db: Firestore;

  before(function () {
    db = getFirestore();
  });

  it('set() replaces serverTimestamp with a concrete Timestamp on read', async function () {
    const ref = db.collection('st_set').doc('d' + Date.now());
    const before = Date.now();
    const writeResult = await ref.set({
      ts: FieldValue.serverTimestamp(),
      name: 'x',
    });
    const after = Date.now();

    expect(writeResult.writeTime).to.be.instanceOf(Timestamp);
    const writeMs = writeResult.writeTime.toMillis();
    expect(writeMs).to.be.at.least(before - 1000);
    expect(writeMs).to.be.at.most(after + 1000);

    const snap = await ref.get();
    const data = snap.data();
    expect(data?.name).to.equal('x');
    expect(data?.ts).to.be.instanceOf(Timestamp);
    expect((data?.ts as Timestamp).toMillis()).to.be.at.least(before - 1000);
    expect((data?.ts as Timestamp).toMillis()).to.be.at.most(after + 1000);
  });

  it('update() with serverTimestamp echoes a writeTime aligned with the stored field', async function () {
    const ref = db.collection('st_update').doc('d' + Date.now());
    await ref.set({ name: 'x' });
    const writeResult = await ref.update({ ts: FieldValue.serverTimestamp() });

    const snap = await ref.get();
    const storedTs = snap.data()?.ts as Timestamp;
    expect(storedTs).to.be.instanceOf(Timestamp);

    // commit_time and the stored field were both generated from the same
    // emulator-clock `Date.now()` in handleCommit. They must align to the
    // same second.
    expect(Math.floor(writeResult.writeTime.toMillis() / 1000)).to.equal(
      Math.floor(storedTs.toMillis() / 1000),
    );
  });

  it('does not leave a serverTimestamp sentinel in the stored doc', async function () {
    const ref = db.collection('st_sentinel').doc('d' + Date.now());
    await ref.set({ ts: FieldValue.serverTimestamp() });

    const snap = await ref.get();
    const value = snap.data()?.ts;
    expect(value).to.not.have.property('_methodName');
    expect(value).to.be.instanceOf(Timestamp);
  });

  it('multiple writes produce monotonically non-decreasing timestamps', async function () {
    const ref = db.collection('st_mono').doc('d' + Date.now());
    await ref.set({ ts: FieldValue.serverTimestamp() });
    const first = (await ref.get()).data()?.ts as Timestamp;

    await new Promise((r) => setTimeout(r, 10));

    await ref.update({ ts: FieldValue.serverTimestamp() });
    const second = (await ref.get()).data()?.ts as Timestamp;

    expect(second.toMillis()).to.be.at.least(first.toMillis());
  });
});
