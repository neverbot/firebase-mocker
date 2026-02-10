/**
 * Express-based HTTP server that emulates the Firebase Auth Identity Toolkit REST API.
 * firebase-admin Auth (getUserByEmail, createUser, deleteUser) uses this when
 * FIREBASE_AUTH_EMULATOR_HOST is set.
 */

import express, { Request, Response } from 'express';
import { getLogger } from '../logger';
import { AuthStorage, AuthEmulatorUser } from './storage';

function randomUid(): string {
  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let uid = '';
  for (let i = 0; i < 28; i++) {
    uid += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return uid;
}

export interface AuthServerConfig {
  port: number;
  host: string;
  projectId?: string;
}

export class AuthServer {
  private readonly storage = new AuthStorage();
  private readonly config: AuthServerConfig;
  private server?: ReturnType<express.Application['listen']>;
  private readonly logger = getLogger();
  private readonly app = express();

  constructor(config: AuthServerConfig) {
    this.config = config;
    this.setupRoutes();
  }

  getStorage(): AuthStorage {
    return this.storage;
  }

  private setupRoutes(): void {
    this.app.use(express.json());

    // Identity Toolkit API: all endpoints under /identitytoolkit.googleapis.com/v1/projects/:projectId/...
    this.app.post(
      '/identitytoolkit.googleapis.com/v1/projects/:projectId/:api',
      (req: Request, res: Response) => {
        void this.handleApi(req, res);
      },
    );

    // Catch-all for non-matching paths
    this.app.use((_req, res) => {
      res.status(404).json({ error: 'Not Found' });
    });

    this.app.use(
      (err: Error, _req: Request, res: Response, _next: () => void) => {
        this.logger.error('error', `[AUTH] Unhandled error: ${err.message}`);
        res.status(500).json({
          error: { message: err.message, code: 500 },
        });
      },
    );
  }

  private async handleApi(req: Request, res: Response): Promise<void> {
    const api = req.params.api;
    const body = (req.body || {}) as Record<string, unknown>;

    const send = (status: number, data: object) => {
      res.status(status).set('Content-Type', 'application/json').json(data);
    };

    try {
      if (api === 'accounts:lookup') {
        this.handleLookup(body, send);
        return;
      }
      if (api === 'accounts') {
        this.handleCreate(body, send);
        return;
      }
      if (api === 'accounts:delete') {
        this.handleDelete(body, send);
        return;
      }
      if (api === 'accounts:update') {
        this.handleUpdate(body, send);
        return;
      }

      send(404, { error: `Unknown API: ${api}` });
    } catch (err) {
      this.logger.error(
        'error',
        `[AUTH] Error handling ${api}: ${err instanceof Error ? err.message : String(err)}`,
      );
      send(500, {
        error: {
          message: err instanceof Error ? err.message : String(err),
          code: 500,
        },
      });
    }
  }

  private handleLookup(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const emailArr = req.email as string[] | undefined;
    const localIdArr = req.localId as string[] | undefined;

    let user: AuthEmulatorUser | undefined;
    if (emailArr && emailArr.length > 0) {
      user = this.storage.getByEmail(emailArr[0]);
    } else if (localIdArr && localIdArr.length > 0) {
      user = this.storage.getByUid(localIdArr[0]);
    }

    if (!user) {
      send(200, { users: [] });
      return;
    }

    send(200, { users: [this.toLookupResponseUser(user)] });
  }

  private toLookupResponseUser(user: AuthEmulatorUser): object {
    return {
      localId: user.localId,
      email: user.email ?? undefined,
      emailVerified: user.emailVerified ?? false,
      displayName: user.displayName ?? undefined,
      photoUrl: user.photoUrl ?? undefined,
      phoneNumber: user.phoneNumber ?? undefined,
      createdAt: user.createdAt,
      lastLoginAt: user.lastLoginAt ?? user.createdAt,
      providerUserInfo: user.providerUserInfo ?? [],
      disabled: user.disabled ?? false,
    };
  }

  private handleCreate(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const email = req.email as string | undefined;
    if (!email || typeof email !== 'string') {
      send(400, { error: { message: 'INVALID_EMAIL', code: 400 } });
      return;
    }

    const existing = this.storage.getByEmail(email);
    if (existing) {
      send(400, {
        error: {
          message: 'The email address is already in use by another account.',
          code: 400,
        },
      });
      return;
    }

    const now = new Date().toISOString();
    const localId = (req.localId as string) || randomUid();
    const user: AuthEmulatorUser = {
      localId,
      email,
      emailVerified: false,
      displayName: (req.displayName as string) || undefined,
      photoUrl: (req.photoUrl as string) || undefined,
      phoneNumber: (req.phoneNumber as string) || undefined,
      createdAt: now,
      lastLoginAt: now,
      providerUserInfo: [
        {
          providerId: 'password',
          rawId: localId,
          email,
          displayName: (req.displayName as string) || undefined,
          photoUrl: (req.photoUrl as string) || undefined,
        },
      ],
      disabled: false,
    };

    this.storage.add(user);
    this.logger.info('server', `[AUTH] Created user ${localId} (${email})`);
    send(200, { localId });
  }

  private handleDelete(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const localId = req.localId as string;
    if (!localId) {
      send(400, { error: { message: 'Missing localId', code: 400 } });
      return;
    }

    const deleted = this.storage.deleteByUid(localId);
    if (deleted) {
      this.logger.info('server', `[AUTH] Deleted user ${localId}`);
    }
    send(200, {});
  }

  private handleUpdate(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const localId = req.localId as string;
    if (!localId) {
      send(400, { error: { message: 'Missing localId', code: 400 } });
      return;
    }

    const user = this.storage.getByUid(localId);
    if (!user) {
      send(400, { error: { message: 'USER_NOT_FOUND', code: 400 } });
      return;
    }

    if (typeof req.email === 'string') {
      user.email = req.email;
    }
    if (typeof req.displayName === 'string') {
      user.displayName = req.displayName;
    }
    if (typeof req.photoUrl === 'string') {
      user.photoUrl = req.photoUrl;
    }
    if (typeof req.phoneNumber === 'string') {
      user.phoneNumber = req.phoneNumber;
    }
    if (typeof req.passwordHash === 'string') {
      user.passwordHash = req.passwordHash;
    }

    this.storage.add(user);
    send(200, { localId });
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.config.port, this.config.host, () => {
        this.logger.info(
          'server',
          `[AUTH] Firebase Auth emulator HTTP server running on http://${this.config.host}:${this.config.port}`,
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
          '[AUTH] Firebase Auth emulator server stopped',
        );
        this.server = undefined;
        resolve();
      });
    });
  }

  /**
   * Debug method to log all content in storage
   * Useful for debugging from external projects
   */
  public debugLog(): void {
    this.storage.debugLog();
  }
}
