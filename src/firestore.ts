/**
 * Mock Firestore implementation
 * Provides a mock Firestore instance that can be used for testing
 */

import { getConfig } from './config';
import { FirestoreStorage } from './firestore-storage';
import { FirestoreServer } from './server';
import { ServerConfig } from './types';

export class MockFirestore {
  private server?: FirestoreServer;
  private readonly storage: FirestoreStorage;
  private readonly config?: Partial<ServerConfig>;

  constructor(config?: Partial<ServerConfig>) {
    this.config = config;
    this.storage = new FirestoreStorage();
  }

  /**
   * Start the Firestore server
   */
  public async start(): Promise<void> {
    if (!this.server) {
      const appConfig = getConfig();
      const serverConfig: ServerConfig = {
        port: this.config?.port ?? appConfig.getPort(),
        host: this.config?.host ?? appConfig.getHost(),
        projectId: this.config?.projectId ?? appConfig.getProjectId(),
      };
      this.server = new FirestoreServer(serverConfig);
      await this.server.start();
    }
  }

  /**
   * Stop the Firestore server
   */
  public async stop(): Promise<void> {
    if (this.server) {
      await this.server.stop();
      this.server = undefined;
    }
  }

  /**
   * Get the storage instance
   */
  public getStorage(): FirestoreStorage {
    return this.storage;
  }

  /**
   * Get the server instance
   */
  public getServer(): FirestoreServer | undefined {
    return this.server;
  }
}
