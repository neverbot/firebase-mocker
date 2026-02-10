/**
 * Main entry point for Firestore and Auth emulator servers
 */

import { config, FirestoreConfig, FirebaseAuthConfig } from './config';
import { AuthServer } from './firebase-auth';
import { FirestoreServer } from './firestore/server';

let authServer: AuthServer | null = null;
let firestoreServer: FirestoreServer | null = null;

/**
 * Main firebaseMocker object with factory methods
 */
export const firebaseMocker = {
  /**
   * Add configuration (firestore, firebase-auth, or logs). Can be called before starting servers.
   */
  addConfig: config.addConfig.bind(config),

  /**
   * Start the Firestore gRPC server.
   * Adds the given config under the "firestore" field (config already has defaults).
   * For logs (e.g. verboseGrpcLogs), call addConfig({ logs: { ... } }) separately.
   * @param opts - Firestore config (port, host, projectId)
   * @returns FirestoreServer instance
   */
  startFirestoreServer: async (
    opts?: Partial<FirestoreConfig>,
  ): Promise<FirestoreServer> => {
    config.addConfig({ firestore: opts });

    const c = config.getObject('firestore');
    process.env.FIRESTORE_EMULATOR_HOST = `${c.host as string}:${c.port as number}`;

    firestoreServer = new FirestoreServer({
      port: c.port as number,
      host: c.host as string,
      projectId: c.projectId as string,
    });
    await firestoreServer.start();
    return firestoreServer;
  },

  /**
   * Start the Firebase Auth HTTP emulator (Identity Toolkit API).
   * Adds the given config under the "firebase-auth" field (config already has defaults).
   * Sets FIREBASE_AUTH_EMULATOR_HOST so firebase-admin Auth uses this emulator.
   * @param opts - Auth config (port, host, projectId)
   * @returns AuthServer instance
   */
  startAuthServer: async (
    opts?: Partial<FirebaseAuthConfig>,
  ): Promise<AuthServer> => {
    config.addConfig({ 'firebase-auth': opts });

    const c = config.getObject('firebase-auth');
    process.env.FIREBASE_AUTH_EMULATOR_HOST = `${c.host as string}:${c.port as number}`;

    authServer = new AuthServer({
      port: c.port as number,
      host: c.host as string,
      projectId: c.projectId as string,
    });
    await authServer.start();
    return authServer;
  },

  /**
   * Stop the last started Auth server (if any).
   */
  stopAuthServer: async (): Promise<void> => {
    if (authServer) {
      await authServer.stop();
      authServer = null;
      delete process.env.FIREBASE_AUTH_EMULATOR_HOST;
    }
  },

  /**
   * Stop the Firestore server
   * @returns Promise that resolves when the server is stopped
   */
  stopFirestoreServer: async (): Promise<void> => {
    if (firestoreServer) {
      await firestoreServer.stop();
      firestoreServer = null;
      delete process.env.FIRESTORE_EMULATOR_HOST;
    }
  },
};
