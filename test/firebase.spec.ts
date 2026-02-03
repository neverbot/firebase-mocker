/**
 * Firebase basic services tests
 * Tests for creating and managing Firestore collections and documents
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { fromFirestoreDocument } from '../src/utils';
import { getFirestore, getFirestoreStorage, setup, teardown } from './_setup';

describe('Firebase Basic Services', () => {
  let db: admin.firestore.Firestore;
  const projectId = 'test-project';
  const databaseId = 'default';

  before(async function () {
    await setup();
    db = getFirestore();
  });

  after(async function () {
    await teardown();
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
      const storedData = fromFirestoreDocument(storedDoc!);
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

        const storedData = fromFirestoreDocument(storedDoc!);
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

      // Create a document
      const orderData = {
        orderId: 'order-123',
        customerId: 'customer-456',
        items: ['item-1', 'item-2'],
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
      const storedData = fromFirestoreDocument(storedDoc!);
      expect(storedData).to.deep.include(orderData);

      // Verify data consistency: Firebase Admin SDK result matches internal storage
      const adminData = doc.data();
      expect(adminData).to.deep.include(storedData);
      expect(storedData).to.deep.include(adminData);
    });
  });
});
