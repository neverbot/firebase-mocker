/**
 * E2E tests for Commit: use the firebase-admin client from _setup
 * to perform batch.commit() (set, update, delete) against the emulator.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, getFirestoreStorage } from '../_setup';

describe('Firestore Commit (e2e)', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(function () {
    db = getFirestore();
  });

  describe('batch set', () => {
    it('batch.set() then commit creates all documents', async function () {
      const batch = db.batch();
      const collectionName = 'commit_e2e_batch_set';
      const doc1 = db.collection(collectionName).doc('d1');
      const doc2 = db.collection(collectionName).doc('d2');

      batch.set(doc1, { name: 'First' });
      batch.set(doc2, { name: 'Second' });
      await batch.commit();

      const snap1 = await doc1.get();
      const snap2 = await doc2.get();
      expect(snap1.exists).to.be.true;
      expect(snap1.data()?.name).to.equal('First');
      expect(snap2.exists).to.be.true;
      expect(snap2.data()?.name).to.equal('Second');
    });
  });

  describe('batch update and delete', () => {
    it('batch.update(), batch.delete(), commit: updates and removes', async function () {
      const collectionName = 'commit_e2e_batch_mixed';
      const doc1 = db.collection(collectionName).doc('d1');
      const doc2 = db.collection(collectionName).doc('d2');

      await doc1.set({ value: 1 });
      await doc2.set({ value: 2 });

      const batch = db.batch();
      batch.update(doc1, { value: 10 });
      batch.delete(doc2);
      await batch.commit();

      const snap1 = await doc1.get();
      const snap2 = await doc2.get();
      expect(snap1.data()?.value).to.equal(10);
      expect(snap2.exists).to.be.false;
    });
  });

  describe('batch write then verify storage', () => {
    it('commit persists to storage', async function () {
      const collectionName = 'commit_e2e_storage';
      const docRef = db.collection(collectionName).doc('d1');

      const batch = db.batch();
      batch.set(docRef, { title: 'Stored' });
      await batch.commit();

      const storage = getFirestoreStorage();
      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        'd1',
      );
      expect(stored).to.exist;
      expect(stored?.fields?.title?.stringValue).to.equal('Stored');
    });
  });
});
