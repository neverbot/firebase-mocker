/**
 * Main entry point for Firestore, Auth, and Storage emulator servers
 */

import {
  config,
  FirestoreConfig,
  FirebaseAuthConfig,
  FirebaseStorageConfig,
  FirebaseRemoteConfigConfig,
} from './config';
import { AuthServer } from './firebase-auth';
import { generateTestIdToken } from './firebase-auth/jwt';
import { RemoteConfigServer } from './firebase-remote-config';
import type { StoredTemplate } from './firebase-remote-config';
import { StorageServer } from './firebase-storage';
import {
  patchGetSignedUrl,
  unpatchGetSignedUrl,
} from './firebase-storage/sign-url-patch';
import { FirestoreServer } from './firestore';

let authServer: AuthServer | null = null;
let firestoreServer: FirestoreServer | null = null;
let storageServer: StorageServer | null = null;
let remoteConfigServer: RemoteConfigServer | null = null;
let previousRemoteConfigUrlBase: string | undefined;

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
   * For logs (e.g. verboseGrpcLogs, verboseAuthLogs), call addConfig({ logs: { ... } }) separately.
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

  /**
   * Start the Firebase Storage HTTP emulator (GCS JSON API).
   * Sets FIREBASE_STORAGE_EMULATOR_HOST so firebase-admin Storage uses this emulator.
   * @param opts - Storage config (port, host, projectId)
   * @returns StorageServer instance
   */
  startStorageServer: async (
    opts?: Partial<FirebaseStorageConfig>,
  ): Promise<StorageServer> => {
    config.addConfig({ 'firebase-storage': opts });

    const c = config.getObject('firebase-storage');
    process.env.FIREBASE_STORAGE_EMULATOR_HOST = `${c.host as string}:${c.port as number}`;

    storageServer = new StorageServer({
      port: c.port as number,
      host: c.host as string,
      projectId: c.projectId as string,
    });
    await storageServer.start();
    patchGetSignedUrl(c.host as string, c.port as number);
    return storageServer;
  },

  /**
   * Stop the last started Storage server (if any).
   */
  stopStorageServer: async (): Promise<void> => {
    if (storageServer) {
      unpatchGetSignedUrl();
      await storageServer.stop();
      storageServer = null;
      delete process.env.FIREBASE_STORAGE_EMULATOR_HOST;
      delete process.env.STORAGE_EMULATOR_HOST;
    }
  },

  /**
   * Start the Firebase Remote Config HTTP emulator.
   * Sets FIREBASE_REMOTE_CONFIG_URL_BASE so firebase-admin RemoteConfig uses this emulator.
   * @param opts - Remote Config config (port, host, projectId, optional initialTemplate)
   * @returns RemoteConfigServer instance
   */
  startRemoteConfigServer: async (
    opts?: Partial<FirebaseRemoteConfigConfig> & {
      initialTemplate?: Partial<StoredTemplate>;
    },
  ): Promise<RemoteConfigServer> => {
    const { initialTemplate, ...rest } = opts ?? {};
    config.addConfig({ 'firebase-remote-config': rest });

    const c = config.getObject('firebase-remote-config');
    previousRemoteConfigUrlBase = process.env.FIREBASE_REMOTE_CONFIG_URL_BASE;
    process.env.FIREBASE_REMOTE_CONFIG_URL_BASE = `http://${c.host as string}:${c.port as number}`;

    remoteConfigServer = new RemoteConfigServer({
      port: c.port as number,
      host: c.host as string,
      projectId: c.projectId as string,
      initialTemplate,
    });
    await remoteConfigServer.start();
    return remoteConfigServer;
  },

  /**
   * Stop the last started Remote Config server (if any).
   */
  stopRemoteConfigServer: async (): Promise<void> => {
    if (remoteConfigServer) {
      await remoteConfigServer.stop();
      remoteConfigServer = null;
      if (previousRemoteConfigUrlBase === undefined) {
        delete process.env.FIREBASE_REMOTE_CONFIG_URL_BASE;
      } else {
        process.env.FIREBASE_REMOTE_CONFIG_URL_BASE =
          previousRemoteConfigUrlBase;
      }
      previousRemoteConfigUrlBase = undefined;
    }
  },

  /**
   * Generate an unsigned test ID token compatible with the Firebase Auth Emulator.
   * The Firebase Admin SDK (in emulator mode) will decode it and accept it
   * via `auth.verifyIdToken()`. The `projectId` must match the one used in
   * `admin.initializeApp({ projectId })`.
   */
  generateTestIdToken,
};
