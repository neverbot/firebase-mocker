/**
 * Mock Firestore implementation
 * Provides a mock Firestore instance that can be used for testing
 */

// Load environment variables from .env file
import dotenv from 'dotenv';
dotenv.config();
import { FirestoreStorage } from './firestore-storage.ts';
import { FirestoreServer } from './server.ts';
import { ServerConfig } from './types.ts';

const DEFAULT_PORT = 3333;
const DEFAULT_HOST = 'localhost';
const DEFAULT_PROJECT_ID = 'demo-project';

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
      const serverConfig: ServerConfig = {
        port:
          this.config?.port ??
          parseInt(process.env.PORT || String(DEFAULT_PORT), 10),
        host: this.config?.host ?? process.env.HOST ?? DEFAULT_HOST,
        projectId:
          this.config?.projectId ??
          process.env.PROJECT_ID ??
          DEFAULT_PROJECT_ID,
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
