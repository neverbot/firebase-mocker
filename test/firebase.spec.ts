/**
 * Firebase basic services tests
 * Tests for creating and managing Firestore collections and documents
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getFirestore, setup, teardown } from './_setup';

describe('Firebase Basic Services', () => {
  let db: admin.firestore.Firestore;

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

      // Verify document was created
      const doc = await docRef.get();
      expect(doc.exists).to.be.true;
      expect(doc.id).to.equal(docId);
      expect(doc.data()).to.deep.include(data);
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

      // Get all documents from the collection
      const snapshot = await collectionRef.get();

      expect(snapshot.empty).to.be.false;
      expect(snapshot.size).to.equal(products.length);
      expect(snapshot.docs).to.have.length(products.length);

      // Verify document data
      snapshot.docs.forEach((doc, index) => {
        expect(doc.exists).to.be.true;
        expect(doc.data()).to.deep.include(products[index]);
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

      // Get the specific document
      const doc = await docRef.get();

      expect(doc.exists).to.be.true;
      expect(doc.id).to.equal(docId);
      expect(doc.data()).to.deep.include(orderData);
    });
  });
});
