/**
 * E2E tests for UpdateDocument: use the firebase-admin client from _setup
 * to perform doc.ref.update() and set(..., { merge: true }) against the emulator.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore UpdateDocument (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('update existing document', () => {
    it('doc.update() merges fields and get returns updated data', async function () {
      const collectionName = 'update_e2e_coll';
      const docId = 'doc-1';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ name: 'Alice', score: 100 });
      await docRef.update({ score: 200, level: 2 });

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.true;
      expect(snapshot.data()?.name).to.equal('Alice');
      expect(snapshot.data()?.score).to.equal(200);
      expect(snapshot.data()?.level).to.equal(2);
    });

    it('updated document is persisted in storage', async function () {
      const collectionName = 'update_e2e_storage';
      const docId = 'doc-1';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ a: 1 });
      await docRef.update({ b: 2 });

      const storage = getFirestoreStorage();
      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.fields?.a?.integerValue).to.equal('1');
      expect(stored?.fields?.b?.integerValue).to.equal('2');
    });
  });

  describe('set with merge', () => {
    it('set(..., { merge: true }) merges with existing document', async function () {
      const collectionName = 'update_e2e_merge';
      const docId = 'doc-1';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ title: 'Original', count: 1 });
      await docRef.set({ count: 2, extra: 'yes' }, { merge: true });

      const snapshot = await docRef.get();
      expect(snapshot.data()?.title).to.equal('Original');
      expect(snapshot.data()?.count).to.equal(2);
      expect(snapshot.data()?.extra).to.equal('yes');
    });
  });

  describe('subcollections', () => {
    it('update document in subcollection', async function () {
      const parentCollection = 'update_e2e_parents';
      const parentId = 'p1';
      const subCollection = 'items';
      const childId = 'i1';

      const parentRef = db.collection(parentCollection).doc(parentId);
      await parentRef.set({ name: 'Parent' });

      const childRef = parentRef.collection(subCollection).doc(childId);
      await childRef.set({ name: 'Item', qty: 1 });
      await childRef.update({ qty: 10 });

      const snapshot = await childRef.get();
      expect(snapshot.data()?.name).to.equal('Item');
      expect(snapshot.data()?.qty).to.equal(10);
    });
  });
});
