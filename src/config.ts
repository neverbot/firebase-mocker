/**
 * Configuration class for firebase-mocker
 * Reads configuration from .env file and provides centralized access
 */

import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

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
 * Configuration class that reads from .env file using dotenv
 */
export class Config {
  private static instance: Config;
  private readonly config: AppConfig;

  private constructor() {
    // Read from environment variables loaded by dotenv from .env file
    const port = this.getNumberConfig('PORT', DEFAULT_PORT);
    const host = this.getStringConfig('HOST', DEFAULT_HOST);
    const projectId = this.getStringConfig('PROJECT_ID', DEFAULT_PROJECT_ID);

    this.config = {
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
   * Get string configuration value
   * Reads from process.env (loaded by dotenv) or returns defaultValue
   */
  private getStringConfig(key: string, defaultValue: string): string {
    if (process.env[key]) {
      return process.env[key];
    }
    return defaultValue;
  }

  /**
   * Get number configuration value
   * Reads from process.env (loaded by dotenv) or returns defaultValue
   */
  private getNumberConfig(key: string, defaultValue: number): number {
    if (process.env[key]) {
      const parsed = parseInt(process.env[key], 10);
      if (!isNaN(parsed)) {
        return parsed;
      }
    }
    return defaultValue;
  }

  /**
   * Get boolean configuration value
   * Reads from process.env (loaded by dotenv) or returns defaultValue
   */
  private getBooleanConfig(key: string, defaultValue: boolean): boolean {
    if (process.env[key]) {
      const value = process.env[key]?.toLowerCase();
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
    return this.config.port;
  }

  /**
   * Get host configuration
   */
  public getHost(): string {
    return this.config.host;
  }

  /**
   * Get project ID configuration
   */
  public getProjectId(): string {
    return this.config.projectId;
  }

  /**
   * Get full configuration object
   */
  public getConfig(): Readonly<AppConfig> {
    return { ...this.config };
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
      port: this.config.port,
      host: this.config.host,
      projectId: this.config.projectId,
    };
  }

  /**
   * Get verbose gRPC logs configuration
   */
  public getVerboseGrpcLogs(): boolean {
    return this.getBooleanConfig('VERBOSE_GRPC_LOGS', false);
  }
}

/**
 * Export singleton instance getter
 */
export function getConfig(): Config {
  return Config.getInstance();
}
