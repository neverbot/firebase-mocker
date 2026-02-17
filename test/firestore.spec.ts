/**
 * Firestore basic services tests
 * Tests for creating and managing Firestore collections and documents
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { fromFirestoreDocument } from '../src/firestore/utils';
import { getFirestore, getFirestoreStorage } from './_setup';

describe('Firestore Basic Services', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = '(default)';

  before(async function () {
    // Setup is already done globally in _setup.ts, just get the Firestore instance
    db = getFirestore();
  });

  describe('Collections', () => {
    it('should create and get a collection', async function () {
      const collectionName = 'test-collection';
      const collectionRef = db.collection(collectionName);

      // Verify collection reference exists
      expect(collectionRef).to.exist;
      expect(collectionRef.id).to.equal(collectionName);
      expect(collectionRef.path).to.equal(collectionName);
    });

    it('should get an empty collection', async function () {
      const collectionName = 'empty-collection';
      const collectionRef = db.collection(collectionName);

      // Get all documents from the collection (should be empty)
      const snapshot = await collectionRef.get();

      expect(snapshot).to.exist;
      expect(snapshot.empty).to.be.true;
      expect(snapshot.size).to.equal(0);
      expect(snapshot.docs).to.be.an('array').that.is.empty;
    });

    it('should create a document in a collection', async function () {
      const collectionName = 'users';
      const docId = 'user-1';
      const collectionRef = db.collection(collectionName);
      const docRef = collectionRef.doc(docId);

      // Create a document
      const data = {
        name: 'John Doe',
        email: 'john@example.com',
        age: 30,
      };

      await docRef.set(data);

      // Verify document was created via Firebase Admin SDK
      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      expect(doc.id).to.equal(docId);
      expect(doc.data()).to.deep.include(data);

      // Verify document exists in internal storage
      const storage = getFirestoreStorage();
      const storedDoc = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(storedDoc).to.exist;
      expect(storedDoc?.name).to.include(collectionName);
      expect(storedDoc?.name).to.include(docId);

      // Verify stored data matches what we set
      const storedData = fromFirestoreDocument(storedDoc);
      expect(storedData).to.deep.include(data);

      // Verify data consistency: Firebase Admin SDK result matches internal storage
      expect(doc.data()).to.deep.include(storedData);
    });

    it('should get a collection with documents', async function () {
      const collectionName = 'products';
      const collectionRef = db.collection(collectionName);

      // Create multiple documents
      const products = [
        { name: 'Product 1', price: 10.99 },
        { name: 'Product 2', price: 20.99 },
        { name: 'Product 3', price: 30.99 },
      ];

      for (const product of products) {
        await collectionRef.add(product);
      }

      // Get all documents from the collection via Firebase Admin SDK
      const snapshot = await collectionRef.get();

      expect(snapshot.empty).to.be.false;
      expect(snapshot.size).to.equal(products.length);
      expect(snapshot.docs).to.have.length(products.length);

      // Verify document data from Firebase Admin SDK
      snapshot.docs.forEach((doc, index) => {
        expect(doc.exists).to.be.true;
        expect(doc.data()).to.deep.include(products[index]);
      });

      // Verify documents exist in internal storage
      const storage = getFirestoreStorage();
      const storedDocs = storage.listDocuments(
        projectId,
        databaseId,
        collectionName,
      );
      expect(storedDocs).to.have.length(products.length);

      // Verify each document in storage matches Firebase Admin SDK results
      snapshot.docs.forEach((adminDoc) => {
        const storedDoc = storage.getDocument(
          projectId,
          databaseId,
          collectionName,
          adminDoc.id,
        );
        expect(storedDoc).to.exist;

        const storedData = fromFirestoreDocument(storedDoc);
        const adminData = adminDoc.data();

        // Verify data consistency
        expect(adminData).to.deep.include(storedData);
        expect(storedData).to.deep.include(adminData);
      });
    });

    it('should get a specific document from a collection', async function () {
      const collectionName = 'orders';
      const docId = 'order-123';
      const collectionRef = db.collection(collectionName);
      const docRef = collectionRef.doc(docId);

      // Create a document with an array field
      // This should work now that we're using JSON protos (same as firebase-admin)
      const orderData = {
        orderId: 'order-123',
        customerId: 'customer-456',
        items: ['item-1', 'item-2'], // Real array, not nested object
        total: 99.99,
        status: 'pending',
      };

      await docRef.set(orderData);

      // Get the specific document via Firebase Admin SDK
      const doc = await docRef.get();

      expect(doc.exists).to.be.true;
      expect(doc.id).to.equal(docId);
      expect(doc.data()).to.deep.include(orderData);

      // Verify document exists in internal storage
      const storage = getFirestoreStorage();
      const storedDoc = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(storedDoc).to.exist;
      expect(storedDoc?.name).to.include(collectionName);
      expect(storedDoc?.name).to.include(docId);

      // Verify stored data matches what we set
      const storedData = fromFirestoreDocument(storedDoc);
      expect(storedData).to.deep.include(orderData);

      // Verify data consistency: Firebase Admin SDK result matches internal storage
      const adminData = doc.data();
      expect(adminData).to.deep.include(storedData);
      expect(storedData).to.deep.include(adminData);
    });
  });

  describe('Document Operations', () => {
    it('should update an existing document', async function () {
      const collectionName = 'users';
      const docId = 'user-update';
      const docRef = db.collection(collectionName).doc(docId);

      // Create initial document
      const initialData = {
        name: 'John Doe',
        email: 'john@example.com',
        age: 30,
      };
      await docRef.set(initialData);

      // Update the document
      const updateData = {
        age: 31,
        city: 'New York',
      };
      await docRef.update(updateData);

      // Verify updated document
      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      const data = doc.data();
      expect(data?.name).to.equal('John Doe'); // Should preserve existing fields
      expect(data?.email).to.equal('john@example.com');
      expect(data?.age).to.equal(31); // Should update this field
      expect(data?.city).to.equal('New York'); // Should add new field
    });

    it('should delete a document', async function () {
      const collectionName = 'temp';
      const docId = 'to-delete';
      const docRef = db.collection(collectionName).doc(docId);

      // Create document
      await docRef.set({ data: 'test' });

      // Verify it exists
      let doc = await docRef.get();
      expect(doc.exists).to.be.true;

      // Delete the document
      await docRef.delete();

      // Verify it's deleted
      doc = await docRef.get();
      expect(doc.exists).to.be.false;

      // Verify it's removed from storage
      const storage = getFirestoreStorage();
      const storedDoc = storage.getDocument(
        projectId,
        databaseId,
        collectionName,
        docId,
      );
      expect(storedDoc).to.be.null;
    });

    it('should handle non-existent document', async function () {
      const collectionName = 'non-existent';
      const docId = 'missing-doc';
      const docRef = db.collection(collectionName).doc(docId);

      // Try to get a non-existent document
      const doc = await docRef.get();
      expect(doc.exists).to.be.false;
      expect(doc.data()).to.be.undefined;
    });
  });

  describe('Data Types', () => {
    it('should handle different data types', async function () {
      const collectionName = 'data-types';
      const docId = 'types-test';
      const docRef = db.collection(collectionName).doc(docId);

      const data = {
        stringValue: 'hello',
        numberValue: 42,
        floatValue: 3.14,
        booleanValue: true,
        booleanFalse: false,
        arrayValue: [1, 2, 3],
      };

      await docRef.set(data);

      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      const retrieved = doc.data();

      expect(retrieved?.stringValue).to.equal('hello');
      expect(retrieved?.numberValue).to.equal(42);
      expect(retrieved?.floatValue).to.equal(3.14);
      expect(retrieved?.booleanValue).to.equal(true);
      expect(retrieved?.booleanFalse).to.equal(false);
      expect(retrieved?.arrayValue).to.deep.equal([1, 2, 3]);
    });

    it('should handle timestamps', async function () {
      const collectionName = 'timestamps';
      const docId = 'timestamp-test';
      const docRef = db.collection(collectionName).doc(docId);

      const now = admin.firestore.Timestamp.now();
      const data = {
        timestamp: now,
      };

      await docRef.set(data);

      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      const retrieved = doc.data();

      expect(retrieved?.timestamp).to.be.instanceOf(admin.firestore.Timestamp);
    });

    it('should handle nested arrays and objects', async function () {
      const collectionName = 'complex-data';
      const docId = 'complex';
      const docRef = db.collection(collectionName).doc(docId);

      const data = {
        users: [
          { name: 'Alice', age: 25 },
          { name: 'Bob', age: 30 },
        ],
        metadata: {
          tags: ['tag1', 'tag2'],
          config: {
            enabled: true,
            count: 5,
          },
        },
      };

      await docRef.set(data);

      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      const retrieved = doc.data();

      expect(retrieved?.users).to.be.an('array').with.length(2);
      expect(retrieved?.users[0]).to.deep.include({ name: 'Alice', age: 25 });
      expect(retrieved?.users[1]).to.deep.include({ name: 'Bob', age: 30 });
      expect(retrieved?.metadata.tags).to.deep.equal(['tag1', 'tag2']);
      expect(retrieved?.metadata.config).to.deep.include({
        enabled: true,
        count: 5,
      });
    });

    it('should handle empty arrays', async function () {
      const collectionName = 'empty-structures';
      const docId = 'empty';
      const docRef = db.collection(collectionName).doc(docId);

      const data = {
        emptyArray: [],
        someValue: 'test',
      };

      await docRef.set(data);

      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      const retrieved = doc.data();

      expect(retrieved?.emptyArray).to.be.an('array').that.is.empty;
      expect(retrieved?.someValue).to.equal('test');
    });
  });

  describe('Subcollections', () => {
    it('should handle subcollections', async function () {
      const collectionName = 'users';
      const docId = 'user-1';
      const subcollectionName = 'posts';
      const subDocId = 'post-1';

      const userRef = db.collection(collectionName).doc(docId);
      const postRef = userRef.collection(subcollectionName).doc(subDocId);

      // Create user document
      await userRef.set({ name: 'John' });

      // Create subcollection document
      const postData = {
        title: 'My First Post',
        content: 'This is the content',
      };
      await postRef.set(postData);

      // Verify subcollection document
      const postDoc = await postRef.get();
      expect(postDoc.exists).to.be.true;
      expect(postDoc.data()).to.deep.include(postData);

      // Verify subcollection path
      expect(postRef.path).to.equal(
        `${collectionName}/${docId}/${subcollectionName}/${subDocId}`,
      );
    });
  });

  describe('Batch Operations', () => {
    it('should handle batch writes', async function () {
      const batch = db.batch();
      const collectionName = 'batch-test';

      const doc1 = db.collection(collectionName).doc('doc1');
      const doc2 = db.collection(collectionName).doc('doc2');
      const doc3 = db.collection(collectionName).doc('doc3');

      batch.set(doc1, { value: 1 });
      batch.set(doc2, { value: 2 });
      batch.set(doc3, { value: 3 });

      await batch.commit();

      // Verify all documents were created
      const [snap1, snap2, snap3] = await Promise.all([
        doc1.get(),
        doc2.get(),
        doc3.get(),
      ]);

      expect(snap1.exists).to.be.true;
      expect(snap1.data()?.value).to.equal(1);
      expect(snap2.exists).to.be.true;
      expect(snap2.data()?.value).to.equal(2);
      expect(snap3.exists).to.be.true;
      expect(snap3.data()?.value).to.equal(3);
    });

    it('should handle batch updates and deletes', async function () {
      const batch = db.batch();
      const collectionName = 'batch-mixed';

      const doc1 = db.collection(collectionName).doc('doc1');
      const doc2 = db.collection(collectionName).doc('doc2');
      const doc3 = db.collection(collectionName).doc('doc3');

      // Create documents first
      await doc1.set({ value: 1 });
      await doc2.set({ value: 2 });
      await doc3.set({ value: 3 });

      // Batch update and delete
      batch.update(doc1, { value: 10 });
      batch.update(doc2, { newField: 'updated' });
      batch.delete(doc3);

      await batch.commit();

      // Verify updates
      const snap1 = await doc1.get();
      expect(snap1.data()?.value).to.equal(10);

      const snap2 = await doc2.get();
      expect(snap2.data()?.value).to.equal(2); // Should preserve existing
      expect(snap2.data()?.newField).to.equal('updated');

      // Verify delete
      const snap3 = await doc3.get();
      expect(snap3.exists).to.be.false;
    });
  });

  describe('Query options (orderBy, limit)', () => {
    it('should return documents sorted by orderBy', async function () {
      const collectionName = 'query-order';
      const col = db.collection(collectionName);
      await col.doc('a').set({ name: 'Alice', score: 10 });
      await col.doc('b').set({ name: 'Bob', score: 30 });
      await col.doc('c').set({ name: 'Carol', score: 20 });

      const snapshot = await col.orderBy('score', 'asc').get();
      expect(snapshot.size).to.equal(3);
      expect(snapshot.docs[0].data()?.score).to.equal(10);
      expect(snapshot.docs[1].data()?.score).to.equal(20);
      expect(snapshot.docs[2].data()?.score).to.equal(30);
    });

    it('should return at most limit documents', async function () {
      const collectionName = 'query-limit';
      const col = db.collection(collectionName);
      await col.doc('1').set({ n: 1 });
      await col.doc('2').set({ n: 2 });
      await col.doc('3').set({ n: 3 });

      const snapshot = await col.limit(2).get();
      expect(snapshot.size).to.equal(2);
    });

    it('should apply orderBy and limit together', async function () {
      const collectionName = 'query-order-limit';
      const col = db.collection(collectionName);
      await col.doc('a').set({ x: 3 });
      await col.doc('b').set({ x: 1 });
      await col.doc('c').set({ x: 2 });

      const snapshot = await col.orderBy('x', 'asc').limit(2).get();
      expect(snapshot.size).to.equal(2);
      const values = snapshot.docs
        .map((d) => d.data()?.x)
        .sort((a, b) => (a as number) - (b as number));
      expect(values).to.deep.equal([1, 2]);
    });
  });

  describe('Storage (internal)', () => {
    it('debugLog does not throw and logs existing data', async function () {
      const storage = getFirestoreStorage();
      const collectionName = 'storage-debug-coll';
      const docId = 'debug-doc';
      const docRef = db.collection(collectionName).doc(docId);
      await docRef.set({
        name: 'Test',
        count: 42,
        active: true,
        tags: ['a', 'b'],
        meta: { nested: true },
      });

      expect(() => storage.debugLog()).to.not.throw();
    });
  });
});
