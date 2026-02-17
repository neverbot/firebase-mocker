/**
 * E2E tests for GetDocument: use the firebase-admin client from _setup
 * to perform doc.get() against the emulator and assert on returned results.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { fromFirestoreDocument } from '../../src/firestore/utils';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore GetDocument (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('get existing document', () => {
    it('returns document when it exists', async function () {
      const collectionName = 'get_doc_coll';
      const docId = 'doc-1';
      const docRef = db.collection(collectionName).doc(docId);

      const data = { name: 'Alice', score: 100 };
      await docRef.set(data);

      const snapshot = await docRef.get();

      expect(snapshot.exists).to.be.true;
      expect(snapshot.id).to.equal(docId);
      expect(snapshot.ref.path).to.equal(`${collectionName}/${docId}`);
      expect(snapshot.data()).to.deep.include(data);
    });

    it('returns document with correct timestamps', async function () {
      const collectionName = 'get_doc_timestamps';
      const docId = 'doc-ts';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.set({ value: 1 });

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.true;

      const created = snapshot.createTime;
      const updated = snapshot.updateTime;
      expect(created).to.exist;
      expect(updated).to.exist;
      expect(created?.toDate()).to.be.instanceOf(Date);
      expect(updated?.toDate()).to.be.instanceOf(Date);
    });

    it('matches internal storage (GetDocument response matches stored document)', async function () {
      const collectionName = 'get_doc_storage';
      const docId = 'doc-stored';
      const docRef = db.collection(collectionName).doc(docId);

      const data = { a: 1, b: 'two', items: [1, 2, 3] };
      await docRef.set(data);

      const snapshot = await docRef.get();
      expect(snapshot.exists).to.be.true;

      const storage = getFirestoreStorage();
      const storedDoc = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(storedDoc).to.exist;

      const storedData = fromFirestoreDocument(storedDoc!);
      expect(snapshot.data()).to.deep.include(storedData);
      expect(storedData).to.deep.include(snapshot.data()!);
    });
  });

  describe('get non-existent document', () => {
    it('returns exists false when document does not exist', async function () {
      const collectionName = 'get_doc_missing_coll';
      const docId = 'no-such-doc';
      const docRef = db.collection(collectionName).doc(docId);

      const snapshot = await docRef.get();

      expect(snapshot.exists).to.be.false;
      expect(snapshot.data()).to.be.undefined;
      expect(snapshot.id).to.equal(docId);
    });

    it('non-existent document is not in storage', async function () {
      const collectionName = 'get_doc_missing_coll';
      const docId = 'no-such-doc';
      const docRef = db.collection(collectionName).doc(docId);

      await docRef.get();

      const storage = getFirestoreStorage();
      const storedDoc = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(storedDoc).to.be.null;
    });
  });

  describe('subcollections', () => {
    it('get document in subcollection returns correct data', async function () {
      const parentCollection = 'parents';
      const parentId = 'p1';
      const subCollection = 'children';
      const childId = 'c1';

      const parentRef = db.collection(parentCollection).doc(parentId);
      await parentRef.set({ name: 'Parent' });

      const childRef = parentRef.collection(subCollection).doc(childId);
      const childData = { name: 'Child', order: 1 };
      await childRef.set(childData);

      const snapshot = await childRef.get();

      expect(snapshot.exists).to.be.true;
      expect(snapshot.id).to.equal(childId);
      expect(snapshot.data()).to.deep.include(childData);
    });
  });
});
