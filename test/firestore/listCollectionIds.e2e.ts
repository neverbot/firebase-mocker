/**
 * E2E tests for ListCollectionIds: use the firebase-admin client from _setup
 * to perform doc.ref.listCollections() against the emulator and assert on results.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore ListCollectionIds (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('document with no subcollections', () => {
    it('returns empty array', async function () {
      const parentRef = db.collection('list_ids_e2e_parents').doc('empty');
      await parentRef.set({ name: 'No subcollections' });

      const collections = await parentRef.listCollections();

      expect(collections).to.be.an('array').that.is.empty;
    });
  });

  describe('document with subcollections', () => {
    it('returns subcollection references with correct ids', async function () {
      const parentRef = db.collection('list_ids_e2e_parents').doc('with_subs');
      await parentRef.set({ name: 'Parent' });
      await parentRef.collection('children').doc('c1').set({ name: 'Child' });
      await parentRef.collection('items').doc('i1').set({ count: 1 });

      const collections = await parentRef.listCollections();

      expect(collections).to.have.length(2);
      const ids = collections.map((c) => c.id).sort();
      expect(ids).to.deep.equal(['children', 'items']);
    });

    it('matches internal storage listCollectionIds', async function () {
      const parentCol = 'list_ids_e2e_match';
      const parentId = 'p1';
      const parentRef = db.collection(parentCol).doc(parentId);
      await parentRef.set({ title: 'Doc' });
      await parentRef.collection('subA').doc('d1').set({});
      await parentRef.collection('subB').doc('d1').set({});

      const collections = await parentRef.listCollections();
      const storage = getFirestoreStorage();
      const pathAfterDocuments = `${parentCol}/${parentId}`;
      const storageIds = storage.listCollectionIds(
        projectId,
        databaseId,
        pathAfterDocuments,
      );

      expect(collections.map((c) => c.id).sort()).to.deep.equal(
        storageIds.slice().sort(),
      );
      expect(collections).to.have.length(2);
    });
  });
});
