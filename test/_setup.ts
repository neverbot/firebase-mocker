/**
 * Test setup file for firebase-mocker
 * This file initializes the Firebase mocker and sets up the testing environment
 */

import * as grpc from '@grpc/grpc-js';
import * as admin from 'firebase-admin';
import { firebaseMocker, FirestoreServer } from '../src/index';

// Initialize Firebase Admin SDK (will use emulator if FIRESTORE_EMULATOR_HOST is set)
let firebaseApp: admin.app.App | undefined = undefined;
let firestoreServer: FirestoreServer | null = null;
let grpcServer: grpc.Server | null = null;

/**
 * Setup function to initialize the Firebase mocker and Firebase Admin SDK
 */
export async function setup(): Promise<void> {
  // Start the Firestore mock server (HTTP REST)
  // This will automatically set FIRESTORE_EMULATOR_HOST
  firestoreServer = await firebaseMocker.startFirestoreServer({
    port: 3333,
    host: 'localhost',
    projectId: 'test-project',
  });

  // The gRPC server is already started by firestoreServer on port 3333
  // So we don't need an additional gRPC server here
  // The main server should be detecting connections

  // Initialize Firebase Admin SDK
  // IMPORTANT: FIRESTORE_EMULATOR_HOST must be set BEFORE initializing the app
  // Firebase Admin SDK checks this variable at initialization time
  if (firebaseApp === undefined) {
    const emulatorHost =
      process.env.FIRESTORE_EMULATOR_HOST || 'localhost:3333';

    // Verify the environment variable is set
    if (!process.env.FIRESTORE_EMULATOR_HOST) {
      process.env.FIRESTORE_EMULATOR_HOST = emulatorHost;
    }

    // Initialize Firebase Admin with explicit project ID
    // Note: We don't need credentials when using emulator
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
    console.log(`[SETUP] Firestore instance created`);

    // Force client creation by creating a collection reference
    // The client is created lazily, so we need to trigger it
    const testCollection = firestore.collection('_setup_test');
    console.log(
      `[SETUP] Created test collection reference to force client initialization: ${testCollection.path}`,
    );
  }

  console.log('Firebase mocker setup complete');
}

/**
 * Teardown function to clean up resources
 */
export async function teardown(): Promise<void> {
  if (grpcServer) {
    grpcServer.forceShutdown();
    grpcServer = null;
    console.log('[TEARDOWN] gRPC server stopped');
  }

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

// Mocha test suite
describe('Firebase Mocker Basic Connection Test', () => {
  before(async function () {
    await setup();
  });

  after(async function () {
    await teardown();
  });

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
