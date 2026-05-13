/**
 * Express-based HTTP server that emulates the Firebase Remote Config REST API.
 * firebase-admin RemoteConfig (getTemplate, publishTemplate) uses this when
 * FIREBASE_REMOTE_CONFIG_URL_BASE points to this server.
 */

import express, { Request, Response } from 'express';
import { getLogger } from '../logger';
import { handleGetTemplate } from './handlers/getTemplate';
import { handleUnimplemented } from './handlers/unimplemented';
import { handleUpdateTemplate } from './handlers/updateTemplate';
import { RemoteConfigStorage, StoredTemplate } from './storage';

export interface RemoteConfigServerConfig {
  port: number;
  host: string;
  projectId: string;
  initialTemplate?: Partial<StoredTemplate>;
}

export class RemoteConfigServer {
  private readonly storage: RemoteConfigStorage;
  private readonly config: RemoteConfigServerConfig;
  private server?: ReturnType<express.Application['listen']>;
  private readonly logger = getLogger();
  private readonly app = express();

  constructor(config: RemoteConfigServerConfig) {
    this.config = config;
    this.storage = new RemoteConfigStorage(
      config.projectId,
      config.initialTemplate,
    );
    this.setupRoutes();
  }

  getStorage(): RemoteConfigStorage {
    return this.storage;
  }

  private setupRoutes(): void {
    this.app.use(express.json());

    this.app.get(
      '/v1/projects/:projectId/remoteConfig',
      (req: Request, res: Response) => {
        handleGetTemplate(this.storage, this.config.projectId, req, res);
      },
    );

    this.app.put(
      '/v1/projects/:projectId/remoteConfig',
      (req: Request, res: Response) => {
        handleUpdateTemplate(this.storage, this.config.projectId, req, res);
      },
    );

    this.app.use(
      '/v1/projects',
      (req: Request, res: Response, next: express.NextFunction) => {
        try {
          handleUnimplemented(req, res);
        } catch (err) {
          next(err);
        }
      },
    );

    this.app.use((_req, res) => {
      res.status(404).json({ error: { code: 404, message: 'Not Found' } });
    });

    this.app.use(
      (err: Error, _req: Request, res: Response, _next: () => void) => {
        this.logger.error(
          'error',
          `[REMOTE-CONFIG] Unhandled error: ${err.message}`,
        );
        res.status(500).json({ error: { message: err.message, code: 500 } });
      },
    );
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.config.port, this.config.host, () => {
        this.logger.info(
          'server',
          `[REMOTE-CONFIG] Firebase Remote Config emulator HTTP server running on http://${this.config.host}:${this.config.port}`,
        );
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.server) {
        resolve();
        return;
      }
      this.server.close(() => {
        this.logger.info(
          'server',
          '[REMOTE-CONFIG] Firebase Remote Config emulator server stopped',
        );
        this.server = undefined;
        resolve();
      });
    });
  }
}
