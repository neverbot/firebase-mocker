/**
 * E2E tests for DeleteDocument: use the firebase-admin client from _setup
 * to perform doc.ref.delete() against the emulator and assert on results.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore DeleteDocument (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('delete existing document', () => {
    it('delete then get returns exists false', async function () {
      const collectionName = 'delete_e2e_coll';
      const docId = 'doc-to-delete';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ name: 'To Delete' });
      await docRef.delete();

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.false;
      expect(snapshot.data()).to.be.undefined;
    });

    it('deleted document is removed from storage', async function () {
      const collectionName = 'delete_e2e_storage';
      const docId = 'doc-removed';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ value: 1 });
      await docRef.delete();

      const storage = getFirestoreStorage();
      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(stored).to.be.null;
    });
  });

  describe('delete non-existent document', () => {
    it('delete on non-existent doc does not throw', async function () {
      const collectionName = 'delete_e2e_missing';
      const docId = 'no-such-doc';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.delete();

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.false;
    });
  });

  describe('subcollections', () => {
    it('delete document in subcollection', async function () {
      const parentCollection = 'delete_e2e_parents';
      const parentId = 'p1';
      const subCollection = 'items';
      const childId = 'i1';

      const parentRef = db.collection(parentCollection).doc(parentId);
      await parentRef.set({ name: 'Parent' });

      const childRef = parentRef.collection(subCollection).doc(childId);
      await childRef.set({ name: 'Item' });
      await childRef.delete();

      const snapshot = await childRef.get();
      expect(snapshot.exists).to.be.false;

      const storage = getFirestoreStorage();
      const path = `${parentCollection}/${parentId}/${subCollection}`;
      expect(storage.getDocument(projectId, databaseId, path, childId)).to.be
        .null;
    });
  });
});
