/**
 * E2E tests for ListDocuments: use the firebase-admin client from _setup
 * to perform collection.get() against the emulator and assert on returned results.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { fromFirestoreDocument } from '../../src/firestore/utils';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore ListDocuments (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('empty collection', () => {
    it('returns empty snapshot when collection has no documents', async function () {
      const collectionName = 'list_e2e_empty';
      const collectionRef = db.collection(collectionName);

      const snapshot = await collectionRef.get();

      expect(snapshot.empty).to.be.true;
      expect(snapshot.size).to.equal(0);
      expect(snapshot.docs).to.be.an('array').that.is.empty;
    });
  });

  describe('collection with documents', () => {
    it('returns all documents with correct data', async function () {
      const collectionName = 'list_e2e_with_docs';
      const collectionRef = db.collection(collectionName);

      const data1 = { name: 'Item A', value: 1 };
      const data2 = { name: 'Item B', value: 2 };
      await collectionRef.doc('a').set(data1);
      await collectionRef.doc('b').set(data2);

      const snapshot = await collectionRef.get();

      expect(snapshot.empty).to.be.false;
      expect(snapshot.size).to.equal(2);
      expect(snapshot.docs).to.have.length(2);

      const ids = snapshot.docs.map((d) => d.id).sort();
      expect(ids).to.deep.equal(['a', 'b']);

      const docA = snapshot.docs.find((d) => d.id === 'a');
      const docB = snapshot.docs.find((d) => d.id === 'b');
      expect(docA?.data()).to.deep.include(data1);
      expect(docB?.data()).to.deep.include(data2);
    });

    it('matches internal storage (snapshot docs match listDocuments)', async function () {
      const collectionName = 'list_e2e_storage';
      const collectionRef = db.collection(collectionName);

      const items = [
        { title: 'First', n: 1 },
        { title: 'Second', n: 2 },
      ];
      await collectionRef.doc('d1').set(items[0]);
      await collectionRef.doc('d2').set(items[1]);

      const snapshot = await collectionRef.get();
      const storage = getFirestoreStorage();
      const storedDocs = storage.listDocuments(
        projectId,
        databaseId,
        collectionName,
      );

      expect(snapshot.size).to.equal(storedDocs.length);

      snapshot.docs.forEach((doc) => {
        const stored = storage.getDocument(
          projectId,
          databaseId,
          collectionName,
          doc.id,
        );
        expect(stored).to.exist;
        const storedData = fromFirestoreDocument(stored!);
        expect(doc.data()).to.deep.include(storedData);
      });
    });
  });

  describe('subcollections', () => {
    it('get subcollection returns documents in subcollection', async function () {
      const parentCollection = 'list_e2e_parents';
      const parentId = 'p1';
      const subCollection = 'items';
      const parentRef = db.collection(parentCollection).doc(parentId);
      await parentRef.set({ name: 'Parent' });

      const subRef = parentRef.collection(subCollection);
      await subRef.doc('i1').set({ name: 'Item 1' });
      await subRef.doc('i2').set({ name: 'Item 2' });

      const snapshot = await subRef.get();

      expect(snapshot.empty).to.be.false;
      expect(snapshot.size).to.equal(2);
      expect(snapshot.docs.map((d) => d.id).sort()).to.deep.equal(['i1', 'i2']);
      expect(snapshot.docs.find((d) => d.id === 'i1')?.data()?.name).to.equal(
        'Item 1',
      );
      expect(snapshot.docs.find((d) => d.id === 'i2')?.data()?.name).to.equal(
        'Item 2',
      );
    });
  });
});
