/**
 * Configuration class for firebase-mocker
 * Supports initialization with a config object or uses defaults
 */

/**
 * Configuration interface
 */
export interface AppConfig {
  port: number;
  host: string;
  projectId: string;
  logs?: {
    verboseGrpcLogs?: boolean;
  };
}

/**
 * Default configuration values
 */
const DEFAULT_CONFIG: AppConfig = {
  port: 3333,
  host: 'localhost',
  projectId: 'demo-project',
  logs: {
    verboseGrpcLogs: false,
  },
};

/**
 * Configuration class that manages application configuration
 */
export class Config {
  private static instance: Config | null = null;
  private readonly appConfig: AppConfig;

  private constructor(config?: Partial<AppConfig>) {
    // Merge provided config with defaults, with environment variables taking precedence
    this.appConfig = {
      port: this.getNumberFromEnv('PORT', config?.port ?? DEFAULT_CONFIG.port),
      host: this.getStringFromEnv('HOST', config?.host ?? DEFAULT_CONFIG.host),
      projectId: this.getStringFromEnv(
        'PROJECT_ID',
        config?.projectId ?? DEFAULT_CONFIG.projectId,
      ),
      logs: {
        verboseGrpcLogs: this.getBooleanFromEnv(
          'LOGS_VERBOSE_GRPC_LOGS',
          config?.logs?.verboseGrpcLogs ??
            DEFAULT_CONFIG.logs?.verboseGrpcLogs ??
            false,
        ),
      },
    };
  }

  /**
   * Initialize the configuration singleton
   * Only initializes if not already initialized
   * @param config - Optional configuration object
   */
  public static initialize(config?: Partial<AppConfig>): void {
    if (!Config.instance) {
      Config.instance = new Config(config);
    }
  }

  /**
   * Get singleton instance (initializes with defaults if not already initialized)
   */
  public static getInstance(): Config {
    if (!Config.instance) {
      Config.instance = new Config();
    }
    return Config.instance;
  }

  /**
   * Get string value from environment variable or return default
   */
  private getStringFromEnv(key: string, defaultValue: string): string {
    return process.env[key] ?? defaultValue;
  }

  /**
   * Get number value from environment variable or return default
   */
  private getNumberFromEnv(key: string, defaultValue: number): number {
    const envValue = process.env[key];
    if (envValue) {
      const parsed = parseInt(envValue, 10);
      if (!isNaN(parsed)) {
        return parsed;
      }
    }
    return defaultValue;
  }

  /**
   * Get boolean value from environment variable or return default
   */
  private getBooleanFromEnv(key: string, defaultValue: boolean): boolean {
    const envValue = process.env[key];
    if (envValue) {
      const value = envValue.toLowerCase();
      if (value === 'true' || value === '1' || value === 'yes') {
        return true;
      }
      if (value === 'false' || value === '0' || value === 'no') {
        return false;
      }
    }
    return defaultValue;
  }

  /**
   * Get port configuration
   */
  public getPort(): number {
    return this.appConfig.port;
  }

  /**
   * Get host configuration
   */
  public getHost(): string {
    return this.appConfig.host;
  }

  /**
   * Get project ID configuration
   */
  public getProjectId(): string {
    return this.appConfig.projectId;
  }

  /**
   * Get full configuration object
   */
  public getConfig(): Readonly<AppConfig> {
    return { ...this.appConfig };
  }

  /**
   * Get ServerConfig compatible object
   */
  public getServerConfig(): {
    port: number;
    host: string;
    projectId: string;
  } {
    return {
      port: this.appConfig.port,
      host: this.appConfig.host,
      projectId: this.appConfig.projectId,
    };
  }

  /**
   * Get a configuration value by path (dot notation)
   * @param path - Path to the configuration value (e.g., 'logs.verboseGrpcLogs', 'port')
   * @returns The configuration value or undefined if not found
   */
  private getValue(path: string): unknown {
    const keys = path.split('.');
    let value: any = this.appConfig;

    for (const key of keys) {
      if (value === null || value === undefined) {
        return undefined;
      }
      value = value[key];
    }

    return value;
  }

  /**
   * Get a boolean configuration value by path
   * @param path - Path to the configuration value (e.g., 'logs.verboseGrpcLogs')
   * @param defaultValue - Default value if not found (default: false)
   * @returns The boolean configuration value
   */
  public getBoolean(path: string, defaultValue = false): boolean {
    const value = this.getValue(path);
    if (typeof value === 'boolean') {
      return value;
    }
    return defaultValue;
  }

  /**
   * Get a string configuration value by path
   * @param path - Path to the configuration value (e.g., 'host', 'projectId')
   * @param defaultValue - Default value if not found
   * @returns The string configuration value
   */
  public getString(path: string, defaultValue = ''): string {
    const value = this.getValue(path);
    if (typeof value === 'string') {
      return value;
    }
    return defaultValue;
  }

  /**
   * Get a number configuration value by path
   * @param path - Path to the configuration value (e.g., 'port')
   * @param defaultValue - Default value if not found
   * @returns The number configuration value
   */
  public getNumber(path: string, defaultValue = 0): number {
    const value = this.getValue(path);
    if (typeof value === 'number') {
      return value;
    }
    return defaultValue;
  }
}

/**
 * Export singleton instance getter
 */
export function getConfig(): Config {
  return Config.getInstance();
}

/**
 * Initialize configuration (should be called before using the package)
 * Only initializes if not already initialized
 */
export function initializeConfig(config?: Partial<AppConfig>): void {
  Config.initialize(config);
}
