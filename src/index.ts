/**
 * Main entry point for Firestore and Auth emulator servers
 */

import { AppConfig, getConfig, initializeConfig } from './config';
import { AuthServer } from './firebase-auth';
import { MockFirestore } from './firestore';
import { FirestoreServer } from './server';
import { ServerConfig } from './types';

let lastAuthServer: AuthServer | null = null;

/**
 * Main firebaseMocker object with factory methods
 */
export const firebaseMocker = {
  /**
   * Create a Mock Firestore instance
   * @param config - Optional server configuration
   * @returns MockFirestore instance
   */
  MockFirestore: (config?: Partial<ServerConfig>): MockFirestore => {
    return new MockFirestore(config);
  },

  /**
   * Start the Firestore server
   * @param config - Server configuration
   * @returns FirestoreServer instance
   */
  startFirestoreServer: async (
    config?: Partial<AppConfig>,
  ): Promise<FirestoreServer> => {
    // Initialize Config with the provided configuration if not already initialized
    if (config) {
      initializeConfig(config);
    }

    const appConfig = getConfig();
    const serverConfig: ServerConfig = {
      port: config?.port ?? appConfig.getPort(),
      host: config?.host ?? appConfig.getHost(),
      projectId: config?.projectId ?? appConfig.getProjectId(),
    };

    // Set FIRESTORE_EMULATOR_HOST environment variable for Firebase SDK
    process.env.FIRESTORE_EMULATOR_HOST = `${serverConfig.host}:${serverConfig.port}`;

    const server = new FirestoreServer(serverConfig);
    await server.start();
    return server;
  },

  /**
   * Start the Firebase Auth emulator (HTTP server implementing Identity Toolkit API).
   * Sets FIREBASE_AUTH_EMULATOR_HOST so firebase-admin Auth uses this emulator.
   * @param config - Optional config with auth.port, auth.host (default 9099, localhost)
   * @returns AuthServer instance (use getStorage() for test helpers, stop() to shut down)
   */
  startAuthServer: async (config?: Partial<AppConfig>): Promise<AuthServer> => {
    if (config) {
      initializeConfig(config);
    }
    const appConfig = getConfig();
    const fullConfig = appConfig.getConfig();
    const authConfig = fullConfig.auth ?? { port: 9099, host: 'localhost' };
    const port = authConfig.port ?? 9099;
    const host = authConfig.host ?? 'localhost';

    const authServer = new AuthServer({
      port,
      host,
      projectId: fullConfig.projectId,
    });
    await authServer.start();
    lastAuthServer = authServer;

    // Firebase Admin SDK expects host:port without protocol
    process.env.FIREBASE_AUTH_EMULATOR_HOST = `${host}:${port}`;
    return authServer;
  },

  /**
   * Stop the last started Auth server (if any).
   */
  stopAuthServer: async (): Promise<void> => {
    if (lastAuthServer) {
      await lastAuthServer.stop();
      lastAuthServer = null;
      delete process.env.FIREBASE_AUTH_EMULATOR_HOST;
    }
  },

  /**
   * Stop the Firestore server
   * @returns Promise that resolves when the server is stopped
   */
  stopFirestoreServer: async (): Promise<void> => {},
};

// Export classes for advanced usage
export { MockFirestore } from './firestore';
export { FirestoreServer } from './server';
export { AuthServer, AuthStorage } from './firebase-auth';
export { Storage } from './storage';
export { AppConfig } from './config';
export * from './types';
