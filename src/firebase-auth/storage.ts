/**
 * In-memory storage for Firebase Auth emulator users.
 * Mirrors the Identity Toolkit API user shape (localId, email, etc.).
 */

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
  }

  listUids(): string[] {
    return Array.from(this.usersByUid.keys());
  }
}
