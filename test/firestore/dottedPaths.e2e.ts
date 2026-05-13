/**
 * E2E tests for update() with dotted field paths.
 *
 * The Admin SDK encodes update({'a.b': v}) as a Write with update_mask.field_paths
 * = ['a.b'] and update.fields containing the nested map. The emulator must apply
 * only the masked paths and preserve sibling fields inside the map.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore dotted field paths (e2e)', () => {
  let db: admin.firestore.Firestore;
  const FieldValue = admin.firestore.FieldValue;

  before(function () {
    db = getFirestore();
  });

  it('update with dotted path preserves sibling fields inside the map', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({
      cart: {
        couponId: 'OLD',
        paymentItems: ['p1', 'p2'],
        items: { a: 1, b: 2 },
      },
      name: 'shop',
    });

    await ref.update({ 'cart.couponId': 'NEW' });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      cart: {
        couponId: 'NEW',
        paymentItems: ['p1', 'p2'],
        items: { a: 1, b: 2 },
      },
      name: 'shop',
    });
  });

  it('supports three-level dotted paths', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({
      a: { b: { c: 'old', d: 'keep' }, other: 'keep2' },
    });

    await ref.update({ 'a.b.c': 'new' });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      a: { b: { c: 'new', d: 'keep' }, other: 'keep2' },
    });
  });

  it('FieldValue.delete() on a dotted path removes only the nested key', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({
      cart: { couponId: 'X', paymentItems: ['p1'], extra: 42 },
    });

    await ref.update({ 'cart.couponId': FieldValue.delete() });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      cart: { paymentItems: ['p1'], extra: 42 },
    });
  });

  it('FieldValue.delete() on top-level field still works', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({ a: 1, b: 2 });

    await ref.update({ a: FieldValue.delete() });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({ b: 2 });
  });

  it('creates intermediate maps when the dotted path does not exist', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({ name: 'shop' });

    await ref.update({ 'cart.couponId': 'NEW' });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      name: 'shop',
      cart: { couponId: 'NEW' },
    });
  });

  it('mixes top-level and dotted paths in the same update', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({
      name: 'old',
      cart: { couponId: 'X', items: ['i1'] },
    });

    await ref.update({
      name: 'new',
      'cart.couponId': 'Y',
    });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      name: 'new',
      cart: { couponId: 'Y', items: ['i1'] },
    });
  });

  it('top-level map update without dotted path still replaces (existing behavior)', async function () {
    const ref = db.collection('dotted').doc('d' + Date.now());
    await ref.set({
      cart: { couponId: 'X', items: ['i1'] },
      name: 'keep',
    });

    await ref.update({ cart: { couponId: 'Y' } });
    const snap = await ref.get();
    expect(snap.data()).to.deep.equal({
      cart: { couponId: 'Y' },
      name: 'keep',
    });
  });
});
