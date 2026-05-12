/**
 * In-memory storage for Firebase Auth emulator users.
 * Mirrors the Identity Toolkit API user shape (localId, email, etc.).
 */

import crypto from 'crypto';
import { getLogger } from '../logger';

export type OobRequestType = 'PASSWORD_RESET' | 'VERIFY_EMAIL' | 'EMAIL_SIGNIN';

export interface OobCodeEntry {
  email: string;
  requestType: OobRequestType;
  createdAt: string;
}

export interface AuthEmulatorUser {
  localId: string;
  email?: string;
  emailVerified?: boolean;
  displayName?: string;
  photoUrl?: string;
  phoneNumber?: string;
  passwordHash?: string;
  salt?: string;
  createdAt: string; // ISO or milliseconds string
  lastLoginAt?: string;
  providerUserInfo?: {
    providerId: string;
    rawId: string;
    email?: string;
    displayName?: string;
    photoUrl?: string;
  }[];
  disabled?: boolean;
}

export class AuthStorage {
  private readonly usersByUid = new Map<string, AuthEmulatorUser>();
  private readonly usersByEmail = new Map<string, string>(); // email -> localId
  private readonly oobCodes = new Map<string, OobCodeEntry>();
  private readonly logger = getLogger();

  getByUid(uid: string): AuthEmulatorUser | undefined {
    return this.usersByUid.get(uid);
  }

  getByEmail(email: string): AuthEmulatorUser | undefined {
    const uid = this.usersByEmail.get(email?.toLowerCase?.() ?? email);
    return uid ? this.usersByUid.get(uid) : undefined;
  }

  add(user: AuthEmulatorUser): void {
    this.usersByUid.set(user.localId, user);
    if (user.email) {
      this.usersByEmail.set(user.email.toLowerCase(), user.localId);
    }
  }

  deleteByUid(uid: string): boolean {
    const user = this.usersByUid.get(uid);
    if (!user) {
      return false;
    }
    if (user.email) {
      this.usersByEmail.delete(user.email.toLowerCase());
    }
    this.usersByUid.delete(uid);
    return true;
  }

  clear(): void {
    this.usersByUid.clear();
    this.usersByEmail.clear();
    this.oobCodes.clear();
  }

  listUids(): string[] {
    return Array.from(this.usersByUid.keys());
  }

  createOobCode(email: string, requestType: OobRequestType): string {
    const code = crypto.randomUUID();
    this.oobCodes.set(code, {
      email,
      requestType,
      createdAt: new Date().toISOString(),
    });
    return code;
  }

  getOobCode(code: string): OobCodeEntry | undefined {
    return this.oobCodes.get(code);
  }

  consumeOobCode(code: string): OobCodeEntry | undefined {
    const entry = this.oobCodes.get(code);
    if (entry) {
      this.oobCodes.delete(code);
    }
    return entry;
  }

  /**
   * Log all users in storage for debugging purposes.
   */
  debugLog(): void {
    if (this.usersByUid.size === 0) {
      this.logger.info('debugAuth', '[AUTH Storage] No users');
      return;
    }
    this.logger.info('debugAuth', '[AUTH Storage] === Users ===');
    this.logger.info('debugAuth', `Total users: ${this.usersByUid.size}`);
    for (const [uid, user] of this.usersByUid.entries()) {
      this.logger.info(
        'auth',
        `  ${uid}: email=${user.email ?? '(none)'} displayName=${user.displayName ?? '(none)'} createdAt=${user.createdAt}`,
      );
    }
    this.logger.info('debugAuth', '[AUTH Storage] === End ===');
  }
}
