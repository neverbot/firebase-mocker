/**
 * Express-based HTTP server that emulates the Firebase Auth Identity Toolkit REST API.
 * firebase-admin Auth (getUserByEmail, createUser, deleteUser) uses this when
 * FIREBASE_AUTH_EMULATOR_HOST is set.
 */

import express, { Request, Response } from 'express';
import { getLogger } from '../logger';
import { generateTestIdToken } from './jwt';
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

  private debugTs(): string {
    return new Date().toISOString();
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
    const api = String(req.params.api);
    const body = (req.body || {}) as Record<string, unknown>;

    // Debug: log every Auth API request
    const bodySummary: Record<string, unknown> = {};
    if (body.email !== undefined) {
      bodySummary.email = body.email;
    }
    if (body.localId !== undefined) {
      bodySummary.localId = body.localId;
    }
    if (Array.isArray(body.email)) {
      bodySummary.email = body.email;
    }
    if (Array.isArray(body.localId)) {
      bodySummary.localId = body.localId;
    }
    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] Incoming ${api} | body keys: ${Object.keys(body).join(',')} | ${JSON.stringify(bodySummary)}`,
    );

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

      if (api === 'accounts:sendOobCode') {
        this.handleSendOobCode(body, send);
        return;
      }

      if (api === 'accounts:signInWithCustomToken') {
        this.handleSignInWithCustomToken(
          body,
          String(req.params.projectId),
          send,
        );
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

    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts:lookup | emailArr=${JSON.stringify(emailArr)} localIdArr=${JSON.stringify(localIdArr)}`,
    );

    let user: AuthEmulatorUser | undefined;
    if (emailArr && emailArr.length > 0) {
      user = this.storage.getByEmail(emailArr[0]);
      this.logger.info(
        'auth',
        `[AUTH DEBUG] [${this.debugTs()}] lookup by email ${emailArr[0]} => ${user ? `found uid=${user.localId}` : 'not found'}`,
      );
    } else if (localIdArr && localIdArr.length > 0) {
      user = this.storage.getByUid(localIdArr[0]);
      this.logger.info(
        'auth',
        `[AUTH DEBUG] [${this.debugTs()}] lookup by localId ${localIdArr[0]} => ${user ? `found email=${user.email}` : 'not found'}`,
      );
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
    const userCount = this.storage.listUids().length;
    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts (create) | email=${email} localId=${req.localId as string} displayName=${req.displayName as string} | storage has ${userCount} users`,
    );
    if (email !== undefined && !email) {
      this.logger.info(
        'auth',
        `[AUTH DEBUG] [${this.debugTs()}] create => 400 INVALID_EMAIL (invalid email)`,
      );
      send(400, { error: { message: 'INVALID_EMAIL', code: 400 } });
      return;
    }

    if (email) {
      const existing = this.storage.getByEmail(email);
      if (existing) {
        const allUids = this.storage.listUids();
        this.logger.info(
          'auth',
          `[AUTH DEBUG] [${this.debugTs()}] create => 400 EMAIL_ALREADY_IN_USE | email=${email} already exists as uid=${existing.localId} (created ${existing.createdAt}) | total users in emulator: ${allUids.length} [${allUids.join(', ')}]`,
        );
        send(400, {
          error: {
            // Identity Toolkit canonical code; firebase-admin v14 maps this
            // to the public `auth/email-already-exists` error with the human
            // message "The email address is already in use by another
            // account." Earlier versions accepted the human message verbatim;
            // since v14's error revamp the SDK requires the canonical token.
            message: 'EMAIL_EXISTS',
            code: 400,
          },
        });
        return;
      }
    }

    const now = new Date().toISOString();
    const localId = (req.localId as string) || randomUid();
    const providerUserInfo = email
      ? [
          {
            providerId: 'password',
            rawId: localId,
            email,
            displayName: (req.displayName as string) || undefined,
            photoUrl: (req.photoUrl as string) || undefined,
          },
        ]
      : [];
    const user: AuthEmulatorUser = {
      localId,
      email,
      emailVerified: false,
      displayName: (req.displayName as string) || undefined,
      photoUrl: (req.photoUrl as string) || undefined,
      phoneNumber: (req.phoneNumber as string) || undefined,
      createdAt: now,
      lastLoginAt: now,
      providerUserInfo,
      disabled: false,
    };

    this.storage.add(user);
    this.logger.info(
      'auth',
      `[AUTH] Created user ${localId} (${email ?? 'no email'})`,
    );
    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] create => 200 localId=${localId}`,
    );
    send(200, { localId });
  }

  private handleDelete(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const localId = req.localId as string;
    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts:delete | localId=${localId}`,
    );
    if (!localId) {
      send(400, { error: { message: 'Missing localId', code: 400 } });
      return;
    }

    const deleted = this.storage.deleteByUid(localId);
    if (deleted) {
      this.logger.info('auth', `[AUTH] Deleted user ${localId}`);
    } else {
      this.logger.info(
        'auth',
        `[AUTH DEBUG] [${this.debugTs()}] delete: uid ${localId} not in storage (no-op)`,
      );
    }
    send(200, {});
  }

  private handleUpdate(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const localId = req.localId as string;
    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts:update | localId=${localId} email=${req.email as string} displayName=${req.displayName as string}`,
    );
    if (!localId) {
      send(400, { error: { message: 'Missing localId', code: 400 } });
      return;
    }

    const user = this.storage.getByUid(localId);
    if (!user) {
      this.logger.info(
        'auth',
        `[AUTH DEBUG] [${this.debugTs()}] update => 400 USER_NOT_FOUND for uid=${localId}`,
      );
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

  private handleSendOobCode(
    req: Record<string, unknown>,
    send: (status: number, data: object) => void,
  ): void {
    const requestType = req.requestType as string;
    const email = req.email as string;

    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts:sendOobCode | email=${email} requestType=${requestType}`,
    );

    if (!email || !requestType) {
      send(400, {
        error: { message: 'INVALID_REQUEST', code: 400 },
      });
      return;
    }

    const oobCode = this.storage.createOobCode(
      email,
      requestType as 'PASSWORD_RESET' | 'VERIFY_EMAIL' | 'EMAIL_SIGNIN',
    );

    const mode =
      requestType === 'PASSWORD_RESET'
        ? 'resetPassword'
        : requestType === 'VERIFY_EMAIL'
          ? 'verifyEmail'
          : 'signIn';

    const oobLink =
      `http://${this.config.host}:${this.config.port}/emulator/action` +
      `?mode=${mode}&oobCode=${oobCode}&apiKey=fake-api-key`;

    this.logger.info(
      'auth',
      `[AUTH] sendOobCode created code=${oobCode} for ${email} (${requestType})`,
    );

    send(200, { email, oobCode, oobLink });
  }

  private handleSignInWithCustomToken(
    req: Record<string, unknown>,
    projectId: string,
    send: (status: number, data: object) => void,
  ): void {
    const token = req.token as string | undefined;

    this.logger.info(
      'auth',
      `[AUTH DEBUG] [${this.debugTs()}] accounts:signInWithCustomToken | hasToken=${Boolean(token)}`,
    );

    if (!token || typeof token !== 'string') {
      send(400, {
        error: { message: 'MISSING_CUSTOM_TOKEN', code: 400 },
      });
      return;
    }

    const parts = token.split('.');
    if (parts.length < 2) {
      send(400, {
        error: { message: 'INVALID_CUSTOM_TOKEN', code: 400 },
      });
      return;
    }

    let payload: Record<string, unknown>;
    try {
      payload = JSON.parse(
        Buffer.from(parts[1], 'base64url').toString('utf8'),
      ) as Record<string, unknown>;
    } catch {
      send(400, {
        error: { message: 'INVALID_CUSTOM_TOKEN', code: 400 },
      });
      return;
    }

    const uid = (payload.uid as string) || (payload.sub as string) || '';
    if (!uid) {
      send(400, {
        error: { message: 'MISSING_CUSTOM_TOKEN_UID', code: 400 },
      });
      return;
    }

    // Auto-create the user if missing — matches the real Auth emulator
    // behavior for `signInWithCustomToken`.
    let user = this.storage.getByUid(uid);
    let isNewUser = false;
    if (!user) {
      const now = new Date().toISOString();
      user = {
        localId: uid,
        createdAt: now,
        lastLoginAt: now,
        emailVerified: false,
        providerUserInfo: [],
        disabled: false,
      };
      this.storage.add(user);
      isNewUser = true;
    }

    const customClaims = payload.claims as Record<string, unknown> | undefined;
    const idToken = generateTestIdToken({
      uid,
      email: user.email,
      projectId,
      claims: customClaims,
    });
    const refreshToken = `refresh-${uid}-${Date.now()}`;

    this.logger.info(
      'auth',
      `[AUTH] signInWithCustomToken uid=${uid} isNewUser=${isNewUser}`,
    );

    send(200, {
      kind: 'identitytoolkit#VerifyCustomTokenResponse',
      idToken,
      refreshToken,
      expiresIn: '3600',
      isNewUser,
      localId: uid,
    });
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
