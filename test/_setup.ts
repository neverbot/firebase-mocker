/**
 * Test setup file for firebase-mocker
 * This file initializes the Firebase mocker and sets up the testing environment
 */

import * as admin from 'firebase-admin';
import { AuthServer } from '../src/firebase-auth';
import { FirestoreServer } from '../src/firestore';
import { firebaseMocker } from '../src/index';

// Initialize Firebase Admin SDK (will use emulators if FIRESTORE_EMULATOR_HOST / FIREBASE_AUTH_EMULATOR_HOST are set)
let firebaseApp: admin.app.App | undefined = undefined;
let firestoreServer: FirestoreServer | null = null;
let authServer: AuthServer | null = null;
let isInitialized = false;
let isTearingDown = false;

/**
 * Setup function to initialize the Firebase mocker and Firebase Admin SDK
 * This function is idempotent - it will only initialize once, even if called multiple times
 */
export async function setup(): Promise<void> {
  // If already initialized, skip
  if (isInitialized) {
    return;
  }

  // Start the Firestore mock server (gRPC)
  firestoreServer = await firebaseMocker.startFirestoreServer({
    port: 3333,
    host: 'localhost',
    projectId: 'test-project',
  });

  // Start the Firebase Auth emulator (HTTP)
  authServer = await firebaseMocker.startAuthServer({
    port: 9099,
    host: 'localhost',
    projectId: 'test-project',
  });

  // IMPORTANT: Emulator env vars must be set BEFORE initializing the app
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    process.env.FIRESTORE_EMULATOR_HOST = 'localhost:3333';
  }
  if (!process.env.FIREBASE_AUTH_EMULATOR_HOST) {
    process.env.FIREBASE_AUTH_EMULATOR_HOST = 'localhost:9099';
  }

  // Initialize Firebase Admin with explicit project ID
  // Note: We don't need credentials when using emulators
  firebaseApp = admin.initializeApp(
    {
      projectId: 'test-project',
    },
    'test-app', // Use a unique app name to avoid conflicts
  );

  // Get Firestore instance
  // Firebase Admin SDK should automatically detect FIRESTORE_EMULATOR_HOST
  const firestore = admin.firestore(firebaseApp);

  // Log the configuration to debug
  console.log(`[SETUP] Firestore admin instance created`);

  // Force client creation by creating a collection reference
  // The client is created lazily, so we need to trigger it
  const testCollection = firestore.collection('_setup_test');
  console.log(
    `[SETUP] Created test collection reference to force client initialization: ${testCollection.path}`,
  );

  isInitialized = true;
  console.log('Firebase mocker setup complete');
}

/**
 * Teardown function to clean up resources
 * This function is idempotent - it will only tear down once, even if called multiple times
 */
export async function teardown(): Promise<void> {
  // If already tearing down or not initialized, skip
  if (isTearingDown || !isInitialized) {
    return;
  }

  isTearingDown = true;

  if (authServer) {
    await firebaseMocker.stopAuthServer();
    authServer = null;
    console.log('[SERVER] Auth server stopped');
  }

  if (firestoreServer) {
    await firestoreServer.stop();
    firestoreServer = null;
    console.log('[SERVER] Firestore server stopped');
  }

  if (firebaseApp !== undefined) {
    await firebaseApp.delete();
    firebaseApp = undefined;
    console.log('[SERVER] Firebase admin app deleted');
  }

  delete process.env.FIREBASE_AUTH_EMULATOR_HOST;
  delete process.env.FIRESTORE_EMULATOR_HOST;

  isInitialized = false;
  isTearingDown = false;
  console.log('[SERVER] Firebase mocker teardown complete');
}

/**
 * Get the Firebase Admin Firestore instance
 */
export function getFirestore(): admin.firestore.Firestore {
  if (!firebaseApp) {
    throw new Error('Firebase app not initialized. Call setup() first.');
  }
  return admin.firestore(firebaseApp);
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
 * Get the Firestore storage instance (for direct access to internal storage)
 */
export function getFirestoreStorage() {
  const server = getFirestoreServer();
  return server.getStorage();
}

/**
 * Get the Firebase Admin app instance (the initialized firebase-admin app)
 */
export function getAdminApp(): admin.app.App {
  if (!firebaseApp) {
    throw new Error('Firebase app not initialized. Call setup() first.');
  }
  return firebaseApp;
}

/**
 * Get the Auth emulator server instance
 */
export function getAuthServer(): AuthServer {
  if (!authServer) {
    throw new Error('Auth server not initialized. Call setup() first.');
  }
  return authServer;
}

/**
 * Get the Auth emulator storage instance (for direct access to internal storage)
 */
export function getAuthStorage() {
  return getAuthServer().getStorage();
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

// Global Mocha hooks - initialize once for all tests
before(async function () {
  await setup();
});

// Global Mocha hook - cleanup after all tests complete
after(async function () {
  await teardown();
});

// Mocha test suite
describe('Firebase Mocker Basic Connection Test', () => {
  it('should connect Firebase Admin SDK to our emulator', async function () {
    console.log('[TEST] Testing Firebase Admin SDK connection...');

    const db = getFirestore();
    const testCollection = db.collection('_connection_test');

    // Try a simple operation: list documents (empty collection is OK)
    // This will attempt to connect to our emulator
    // If connection fails, this will throw an error or timeout
    try {
      // Add error listeners to catch any connection errors
      process.on('unhandledRejection', (reason, _promise) => {
        console.error('[TEST] Unhandled rejection:', reason);
      });

      process.on('uncaughtException', (error) => {
        console.error('[TEST] Uncaught exception:', error);
      });

      let timeoutId: NodeJS.Timeout | null = null;
      const timeoutPromise = new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => {
          console.log('[TEST] Connection timeout - no response from emulator');
          console.log(
            '[TEST] This suggests Firebase Admin SDK is not attempting to connect',
          );
          reject(new Error('Connection timeout - emulator not responding'));
        }, 5000);
      });

      const _snapshot = await Promise.race([
        testCollection.get().catch((error) => {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          console.error('[TEST] Collection.get() error:', error);
          console.error('[TEST] Error code:', error.code);
          console.error('[TEST] Error message:', error.message);
          console.error('[TEST] Error details:', error);
          throw error;
        }),
        timeoutPromise,
      ]);

      // Clear timeout if we got here successfully
      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      // If we get here, connection was successful
      // The snapshot might be empty, which is fine
      console.log(
        '✓ Connection successful: Firebase Admin SDK connected to our emulator',
      );
    } catch (error) {
      // If connection fails, this test should fail
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown connection error';
      console.error('[TEST] Connection error:', errorMessage);
      console.error('[TEST] Error details:', error);
      throw new Error(`Failed to connect to emulator: ${errorMessage}.`);
    }
  });
});

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
