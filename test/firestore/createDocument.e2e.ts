/**
 * E2E tests for CreateDocument: use the firebase-admin client from _setup
 * to perform collection.add() and doc.set() against the emulator and assert on results.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore CreateDocument (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('create with set (explicit id)', () => {
    it('doc.set() creates document and get returns data', async function () {
      const collectionName = 'create_e2e_set';
      const docId = 'doc-1';
      const docRef = db.collection(collectionName).doc(docId);

      const data = { name: 'Alice', score: 100 };
      await docRef.set(data);

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.true;
      expect(snapshot.id).to.equal(docId);
      expect(snapshot.data()).to.deep.include(data);
    });

    it('created document is in storage', async function () {
      const collectionName = 'create_e2e_storage';
      const docId = 'stored-doc';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ title: 'Stored', n: 1 });

      const storage = getFirestoreStorage();
      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.fields?.title?.stringValue).to.equal('Stored');
    });
  });

  describe('create with add (auto id)', () => {
    it('collection.add() creates document with generated id', async function () {
      const collectionName = 'create_e2e_add';
      const collectionRef = db.collection(collectionName);

      const data = { name: 'Auto', value: 1 };
      const docRef = await collectionRef.add(data);

      expect(docRef.id).to.exist;
      expect(docRef.id).to.have.lengthOf.above(0);

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.true;
      expect(snapshot.data()).to.deep.include(data);
    });
  });

  describe('subcollections', () => {
    it('create document in subcollection via set', async function () {
      const parentCollection = 'create_e2e_parents';
      const parentId = 'p1';
      const subCollection = 'items';
      const childId = 'i1';

      const parentRef = db.collection(parentCollection).doc(parentId);
      await parentRef.set({ name: 'Parent' });

      const childRef = parentRef.collection(subCollection).doc(childId);
      const childData = { name: 'Item', order: 1 };
      await childRef.set(childData);

      const snapshot = await childRef.get();
      expect(snapshot.exists).to.be.true;
      expect(snapshot.data()).to.deep.include(childData);

      const storage = getFirestoreStorage();
      const path = `${parentCollection}/${parentId}/${subCollection}`;
      const stored = storage.getDocument(projectId, databaseId, path, childId);
      expect(stored).to.exist;
    });
  });
});
