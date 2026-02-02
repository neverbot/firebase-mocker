/**
 * Configuration class for firebase-mocker
 * Reads configuration from config/*.yml files using 'config' package
 */

import config from 'config';

/**
 * Default configuration values
 */
const DEFAULT_PORT = 3333;
const DEFAULT_HOST = 'localhost';
const DEFAULT_PROJECT_ID = 'demo-project';

/**
 * Configuration interface
 */
export interface AppConfig {
  port: number;
  host: string;
  projectId: string;
}

/**
 * Configuration class that reads from config/*.yml files using 'config' package
 */
export class Config {
  private static instance: Config;
  private readonly appConfig: AppConfig;

  private constructor() {
    // Read from config package (reads from config/*.yml files based on NODE_ENV)
    // Priority: environment variables > config files > defaults
    const port = this.getNumberConfig('port', DEFAULT_PORT);
    const host = this.getStringConfig('host', DEFAULT_HOST);
    const projectId = this.getStringConfig('projectId', DEFAULT_PROJECT_ID);

    this.appConfig = {
      port,
      host,
      projectId,
    };
  }

  /**
   * Get singleton instance
   */
  public static getInstance(): Config {
    if (!Config.instance) {
      Config.instance = new Config();
    }
    return Config.instance;
  }

  /**
   * Convert camelCase or dot notation to UPPER_SNAKE_CASE
   * Examples: "verboseGrpcLogs" -> "VERBOSE_GRPC_LOGS"
   *           "logs.verboseGrpcLogs" -> "LOGS_VERBOSE_GRPC_LOGS"
   */
  private toEnvKey(key: string): string {
    // Replace dots with underscores, then convert camelCase to SNAKE_CASE
    return key
      .replace(/\./g, '_')
      .replace(/([A-Z])/g, '_$1')
      .toUpperCase();
  }

  /**
   * Get string configuration value
   * Priority: process.env > config package > defaultValue
   */
  private getStringConfig(key: string, defaultValue: string): string {
    // First try environment variable (uppercase, with underscores)
    const envKey = this.toEnvKey(key);
    if (process.env[envKey]) {
      return process.env[envKey];
    }

    // Then try config package
    try {
      if (config.has(key)) {
        return config.get<string>(key);
      }
    } catch {
      // Ignore if key doesn't exist in config
    }

    return defaultValue;
  }

  /**
   * Get number configuration value
   * Priority: process.env > config package > defaultValue
   */
  private getNumberConfig(key: string, defaultValue: number): number {
    // First try environment variable (uppercase, with underscores)
    const envKey = this.toEnvKey(key);
    if (process.env[envKey]) {
      const parsed = parseInt(process.env[envKey], 10);
      if (!isNaN(parsed)) {
        return parsed;
      }
    }

    // Then try config package
    try {
      if (config.has(key)) {
        return config.get<number>(key);
      }
    } catch {
      // Ignore if key doesn't exist in config
    }

    return defaultValue;
  }

  /**
   * Get boolean configuration value
   * Priority: process.env > config package > defaultValue
   */
  private getBooleanConfig(key: string, defaultValue: boolean): boolean {
    // First try environment variable (uppercase, with underscores)
    const envKey = this.toEnvKey(key);
    if (process.env[envKey]) {
      const value = process.env[envKey]?.toLowerCase();
      if (value === 'true' || value === '1' || value === 'yes') {
        return true;
      }
      if (value === 'false' || value === '0' || value === 'no') {
        return false;
      }
    }

    // Then try config package
    try {
      if (config.has(key)) {
        return config.get<boolean>(key);
      }
    } catch {
      // Ignore if key doesn't exist in config
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
   * Get verbose gRPC logs configuration
   */
  public getVerboseGrpcLogs(): boolean {
    return this.getBooleanConfig('logs.verboseGrpcLogs', false);
  }
}

/**
 * Export singleton instance getter
 */
export function getConfig(): Config {
  return Config.getInstance();
}
