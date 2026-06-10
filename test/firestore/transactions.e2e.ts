/**
 * E2E tests for db.runTransaction() against the firebase-mocker emulator.
 * Level 1 semantics: atomic commits, no conflict detection.
 */

import { expect } from 'chai';
import { Firestore } from 'firebase-admin/firestore';
import { getFirestore } from '../_setup';

describe('Firestore transactions (e2e)', () => {
  let db: Firestore;

  before(function () {
    db = getFirestore();
  });

  it('runTransaction with read and write applies changes', async function () {
    const col = db.collection(`txn-rw-${Date.now()}`);
    await col.doc('counter').set({ value: 0 });

    await db.runTransaction(async (txn) => {
      const snap = await txn.get(col.doc('counter'));
      const current = (snap.data()?.value as number | undefined) ?? 0;
      txn.update(col.doc('counter'), { value: current + 1 });
    });

    const final = await col.doc('counter').get();
    expect(final.data()?.value).to.equal(1);
  });

  it('runTransaction with multiple writes commits all atomically', async function () {
    const col = db.collection(`txn-multi-${Date.now()}`);
    await db.runTransaction(async (txn) => {
      txn.set(col.doc('a'), { v: 1 });
      txn.set(col.doc('b'), { v: 2 });
      txn.set(col.doc('c'), { v: 3 });
    });

    const all = await col.get();
    expect(all.size).to.equal(3);
  });

  it('runTransaction throwing in callback rolls back (no writes persist)', async function () {
    const col = db.collection(`txn-rollback-${Date.now()}`);
    await col.doc('keep').set({ v: 'original' });

    try {
      await db.runTransaction(async (txn) => {
        txn.update(col.doc('keep'), { v: 'changed' });
        throw new Error('user error');
      });
      expect.fail('expected runTransaction to throw');
    } catch (err: unknown) {
      const message = (err as Error)?.message ?? String(err);
      expect(message).to.include('user error');
    }

    const after = await col.doc('keep').get();
    expect(after.data()?.v).to.equal('original');
  });

  it('runTransaction reading non-existent document returns exists=false', async function () {
    const col = db.collection(`txn-missing-${Date.now()}`);
    let exists = true;
    await db.runTransaction(async (txn) => {
      const snap = await txn.get(col.doc('missing'));
      exists = snap.exists;
    });
    expect(exists).to.be.false;
  });

  it('runTransaction with create, update, delete', async function () {
    const col = db.collection(`txn-crud-${Date.now()}`);
    await col.doc('a').set({ v: 1 });
    await col.doc('b').set({ v: 2 });

    await db.runTransaction(async (txn) => {
      txn.set(col.doc('c'), { v: 3 });
      txn.update(col.doc('a'), { v: 11 });
      txn.delete(col.doc('b'));
    });

    const snapshot = await col.get();
    const data: Record<string, number> = {};
    for (const d of snapshot.docs) {
      data[d.id] = d.data().v as number;
    }
    expect(data).to.deep.equal({ a: 11, c: 3 });
  });

  it('runTransaction with txn.get(query) works (RunQuery in txn context)', async function () {
    const col = db.collection(`txn-query-${Date.now()}`);
    await col.doc('a').set({ score: 10 });
    await col.doc('b').set({ score: 20 });

    let count = 0;
    await db.runTransaction(async (txn) => {
      const q = await txn.get(col.where('score', '>=', 10));
      count = q.size;
    });

    expect(count).to.equal(2);
  });

  it('two sequential runTransactions both succeed and persist', async function () {
    const col = db.collection(`txn-seq-${Date.now()}`);
    await col.doc('counter').set({ value: 0 });

    await db.runTransaction(async (txn) => {
      const snap = await txn.get(col.doc('counter'));
      const v = (snap.data()?.value as number | undefined) ?? 0;
      txn.update(col.doc('counter'), { value: v + 1 });
    });

    await db.runTransaction(async (txn) => {
      const snap = await txn.get(col.doc('counter'));
      const v = (snap.data()?.value as number | undefined) ?? 0;
      txn.update(col.doc('counter'), { value: v + 1 });
    });

    const final = await col.doc('counter').get();
    expect(final.data()?.value).to.equal(2);
  });
});
