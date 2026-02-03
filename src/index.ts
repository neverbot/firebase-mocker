/**
 * Main entry point for Firestore emulator server
 */

import { MockAuthentication } from './auth';
import { getConfig } from './config';
import { MockFirestore } from './firestore';
import { FirestoreServer } from './server';
import { ServerConfig } from './types';

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
   * Create a Mock Authentication instance
   * @returns MockAuthentication instance
   */
  MockAuthentication: (): MockAuthentication => {
    return new MockAuthentication();
  },

  /**
   * Start the Firestore server
   * @param config - Server configuration
   * @returns FirestoreServer instance
   */
  startFirestoreServer: async (
    config?: Partial<ServerConfig>,
  ): Promise<FirestoreServer> => {
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
   * Stop the Firestore server
   * @returns Promise that resolves when the server is stopped
   */
  stopFirestoreServer: async (): Promise<void> => {},
};

// Export classes for advanced usage
export { MockFirestore } from './firestore';
export { MockAuthentication } from './auth';
export { FirestoreServer } from './server';
export { Storage } from './storage';
export * from './types';
