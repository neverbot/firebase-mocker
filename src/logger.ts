/**
 * Logger utility using Winston
 * Provides centralized logging with support for different log types
 */

import winston from 'winston';
import { config } from './config';

/**
 * Log message types
 */
export type LogType =
  | 'setup'
  | 'test'
  | 'grpc'
  | 'auth'
  | 'server'
  | 'error'
  | 'info';

/**
 * Logger class that wraps Winston
 */
export class Logger {
  private readonly logger: winston.Logger;

  constructor() {
    // Create Winston logger with simple format
    this.logger = winston.createLogger({
      level: 'info',
      format: winston.format.combine(
        winston.format.printf((info) => {
          // Format similar to previous console.log format
          const type = info.type as string | undefined;
          const message = info.message as string;
          const typePrefix = type ? `[${type.toUpperCase()}]` : '';
          return typePrefix ? `${typePrefix} ${message}` : message;
        }),
      ),
      transports: [
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.printf((info) => {
              // Format similar to previous console.log format
              const type = info.type as string | undefined;
              const message = info.message as string;
              const typePrefix = type ? `[${type.toUpperCase()}]` : '';
              return typePrefix ? `${typePrefix} ${message}` : message;
            }),
          ),
        }),
      ],
    });
  }

  /**
   * Log a message with a specific type
   * @param type - Type of log message (setup, test, grpc, server, error, info)
   * @param message - Message content
   * @param level - Log level (default: 'info')
   */
  public log(
    type: LogType,
    message: string,
    level: 'error' | 'warn' | 'info' | 'debug' = 'info',
  ): void {
    // Check if we should log gRPC messages (read config dynamically)
    if (type === 'grpc' && !config.getBoolean('logs.verboseGrpcLogs')) {
      return;
    }
    // Check if we should log Firebase Auth emulator messages
    if (type === 'auth' && !config.getBoolean('logs.verboseAuthLogs')) {
      return;
    }

    this.logger.log({
      level,
      message,
      type,
    });
  }

  /**
   * Log an error message
   */
  public error(type: LogType, message: string): void {
    this.log(type, message, 'error');
  }

  /**
   * Log a warning message
   */
  public warn(type: LogType, message: string): void {
    this.log(type, message, 'warn');
  }

  /**
   * Log an info message
   */
  public info(type: LogType, message: string): void {
    this.log(type, message, 'info');
  }

  /**
   * Log a debug message
   */
  public debug(type: LogType, message: string): void {
    this.log(type, message, 'debug');
  }

  /**
   * Log a message directly without specifying type
   * Useful for simple logging that doesn't need categorization
   */
  public logMessage(message: string): void {
    this.logger.log({
      level: 'info',
      message,
    });
  }
}

/**
 * Singleton instance
 */
let loggerInstance: Logger | null = null;

/**
 * Get logger instance
 */
export function getLogger(): Logger {
  if (!loggerInstance) {
    loggerInstance = new Logger();
  }
  return loggerInstance;
}

/**
 * Convenience function to log messages
 */
export function log(
  type: LogType,
  message: string,
  level: 'error' | 'warn' | 'info' | 'debug' = 'info',
): void {
  getLogger().log(type, message, level);
}
