/**
 * Main entry point for Firestore emulator server
 */

import dotenv from 'dotenv';
import { MockAuthentication } from './auth';
import { MockFirestore } from './firestore';
import { FirestoreServer } from './server';
import { MockStorage } from './storage';
import { ServerConfig } from './types';

const DEFAULT_PORT = 3333;
const DEFAULT_HOST = 'localhost';
const DEFAULT_PROJECT_ID = 'demo-project';

// Load environment variables from .env file
dotenv.config();

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
   * Create a Mock Storage instance
   * @returns MockStorage instance
   */
  MockStorage: (): MockStorage => {
    return new MockStorage();
  },

  /**
   * Start the Firestore server
   * @param config - Server configuration
   * @returns FirestoreServer instance
   */
  startFirestoreServer: async (
    config?: Partial<ServerConfig>,
  ): Promise<FirestoreServer> => {
    const serverConfig: ServerConfig = {
      port:
        config?.port ?? parseInt(process.env.PORT || String(DEFAULT_PORT), 10),
      host: config?.host ?? process.env.HOST ?? DEFAULT_HOST,
      projectId:
        config?.projectId ?? process.env.PROJECT_ID ?? DEFAULT_PROJECT_ID,
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
export { MockStorage } from './storage';
export { FirestoreServer } from './server';
export { FirestoreStorage } from './firestore-storage';
export * from './types';
