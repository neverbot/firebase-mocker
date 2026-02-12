/**
 * Configuration for firebase-mocker.
 * No environment variables: all values are passed as parameters.
 */

/**
 * Firestore emulator configuration (gRPC server)
 */
export interface FirestoreConfig {
  port: number;
  host: string;
  projectId: string;
}

/**
 * Firebase Auth emulator configuration (HTTP server)
 */
export interface FirebaseAuthConfig {
  port: number;
  host: string;
  projectId?: string;
}

/**
 * Optional configuration for the emulators (e.g. default logs)
 */
export interface CommonConfig {
  logs?: {
    verboseGrpcLogs?: boolean;
    verboseAuthLogs?: boolean;
    /** When an unimplemented RPC is called: 'warn' = log to stderr and return UNIMPLEMENTED, 'throw' = throw so process fails */
    onUnimplemented?: 'warn' | 'throw';
  };
}

/**
 * Partial config you can pass to addConfig.
 * Use field "firestore" for Firestore options, "firebase-auth" for Auth options.
 */
export interface Configuration {
  firestore?: Partial<FirestoreConfig>;
  'firebase-auth'?: Partial<FirebaseAuthConfig>;
  logs?: CommonConfig['logs'];
}

export const DEFAULT_FIRESTORE: FirestoreConfig = {
  port: 3333,
  host: 'localhost',
  projectId: 'demo-project',
};

export const DEFAULT_FIREBASE_AUTH: Required<FirebaseAuthConfig> = {
  port: 9099,
  host: 'localhost',
  projectId: 'demo-project',
};

/**
 * Singleton configuration. On first creation, storage is initialized with
 * DEFAULT_FIRESTORE and DEFAULT_FIREBASE_AUTH.
 */
class Config {
  private static instance: Config | null = null;

  private readonly storage: Configuration;

  private constructor() {
    this.storage = {
      firestore: { ...DEFAULT_FIRESTORE },
      'firebase-auth': { ...DEFAULT_FIREBASE_AUTH },
      logs: {
        verboseGrpcLogs: false,
        verboseAuthLogs: false,
        onUnimplemented: 'warn',
      },
    };
  }

  static getInstance(): Config {
    if (!Config.instance) {
      Config.instance = new Config();
    }
    return Config.instance;
  }

  addConfig(patch?: Configuration): void {
    if (!patch) {
      return;
    }
    if (patch.firestore !== undefined) {
      this.storage.firestore = {
        ...this.storage.firestore,
        ...patch.firestore,
      };
    }
    if (patch['firebase-auth'] !== undefined) {
      this.storage['firebase-auth'] = {
        ...this.storage['firebase-auth'],
        ...patch['firebase-auth'],
      };
    }
    if (patch.logs !== undefined) {
      this.storage.logs = { ...this.storage.logs, ...patch.logs };
    }
  }

  /**
   * Get a value by dot-notation path (e.g. "firestore.port", "logs.verboseGrpcLogs", "logs.verboseAuthLogs", "firebase-auth.host").
   * Resolves against storage only. Returns undefined if the path is missing.
   */
  private getByPath(path: string): unknown {
    const parts = path.split('.');
    let current: unknown = this.storage as Record<string, unknown>;
    for (const part of parts) {
      if (current === null || current === undefined) {
        return undefined;
      }
      current = (current as Record<string, unknown>)[part];
    }
    return current;
  }

  getString(path: string, defaultValue = ''): string {
    const value = this.getByPath(path);
    return typeof value === 'string' ? value : defaultValue;
  }

  getNumber(path: string, defaultValue = 0): number {
    const value = this.getByPath(path);
    if (typeof value === 'number' && !Number.isNaN(value)) {
      return value;
    }
    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      return Number.isNaN(parsed) ? defaultValue : parsed;
    }
    return defaultValue;
  }

  getBoolean(path: string, defaultValue = false): boolean {
    const value = this.getByPath(path);
    if (typeof value === 'boolean') {
      return value;
    }
    if (value === 'true' || value === '1') {
      return true;
    }
    if (value === 'false' || value === '0') {
      return false;
    }
    return defaultValue;
  }

  getObject(
    path: string,
    defaultValue: Record<string, unknown> = {},
  ): Record<string, unknown> {
    const value = this.getByPath(path);
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      return value as Record<string, unknown>;
    }
    return defaultValue;
  }
}

/**
 * Single config API. Use config.getBoolean(path), config.getString(path), config.addConfig(patch), etc.
 */
export const config = Config.getInstance();
