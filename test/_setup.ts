/**
 * Test setup file for firebase-mocker
 * This file initializes the Firebase mocker and sets up the testing environment
 */

import * as admin from 'firebase-admin';
import { firebaseMocker, FirestoreServer } from '../src/index';

// Initialize Firebase Admin SDK (will use emulator if FIRESTORE_EMULATOR_HOST is set)
let firebaseApp: admin.app.App | undefined = undefined;
let firestoreServer: FirestoreServer | null = null;

/**
 * Setup function to initialize the Firebase mocker and Firebase Admin SDK
 */
export async function setup(): Promise<void> {
  // Start the Firestore mock server
  // This will automatically set FIRESTORE_EMULATOR_HOST
  firestoreServer = await firebaseMocker.startFirestoreServer({
    port: 3333,
    host: 'localhost',
    projectId: 'test-project',
  });

  // Initialize Firebase Admin SDK
  // The FIRESTORE_EMULATOR_HOST should be set automatically by startFirestoreServer
  if (firebaseApp === undefined) {
    firebaseApp = admin.initializeApp({
      projectId: 'test-project',
    });
  }

  console.log('Firebase mocker setup complete');

  console.log(
    `FIRESTORE_EMULATOR_HOST: ${process.env.FIRESTORE_EMULATOR_HOST}`,
  );
}

/**
 * Teardown function to clean up resources
 */
export async function teardown(): Promise<void> {
  if (firestoreServer) {
    await firestoreServer.stop();
    firestoreServer = null;
  }

  if (firebaseApp !== undefined) {
    await firebaseApp.delete();
    firebaseApp = undefined;
  }

  // Clean up environment variable
  delete process.env.FIRESTORE_EMULATOR_HOST;

  console.log('Firebase mocker teardown complete');
}

/**
 * Get the Firebase Admin Firestore instance
 */
export function getFirestore(): admin.firestore.Firestore {
  if (!firebaseApp) {
    throw new Error('Firebase app not initialized. Call setup() first.');
  }
  return admin.firestore();
}

/**
 * Get the Firestore server instance
 */
export function getFirestoreServer(): FirestoreServer {
  if (!firestoreServer) {
    throw new Error('Firestore server not initialized. Call setup() first.');
  }
  return firestoreServer;
}

/**
 * Test function to verify the setup works
 */
export async function testSetup(): Promise<void> {
  await setup();

  try {
    const db = getFirestore();
    const testCollection = db.collection('_test');

    // Test write
    const testDoc = testCollection.doc('test-doc');
    await testDoc.set({
      message: 'Hello from firebase-mocker',
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
    });

    // Test read
    const doc = await testDoc.get();
    if (!doc.exists) {
      throw new Error('Document was not created');
    }

    const data = doc.data();
    if (!data || data.message !== 'Hello from firebase-mocker') {
      throw new Error('Document data does not match');
    }

    // Clean up test document
    await testDoc.delete();

    console.log('✓ Setup test passed: Firestore operations working correctly');
  } catch (error) {
    console.error('✗ Setup test failed:', error);
    throw error;
  } finally {
    await teardown();
  }
}

// If this file is run directly, execute the test
if (require.main === module) {
  testSetup()
    .then(() => {
      console.log('Setup test completed successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Setup test failed:', error);
      process.exit(1);
    });
}
