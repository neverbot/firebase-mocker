/**
 * Unsigned JWT helper for generating test ID tokens.
 *
 * The Firebase Auth Emulator uses JWTs with `alg: 'none'` (no signature).
 * The Firebase Admin SDK in emulator mode decodes them locally without
 * verifying the signature, but still validates `iss`, `aud`, `sub`, `exp`.
 *
 * Use this helper in tests to produce tokens that `auth.verifyIdToken()`
 * will accept.
 */

export interface TestIdTokenOptions {
  /** User ID (becomes `sub` and `uid` claims). */
  uid: string;
  /** Optional email claim. */
  email?: string;
  /** Must match the projectId passed to `admin.initializeApp(...)`. */
  projectId: string;
  /** Seconds until expiration. Default: 3600 (1 hour). */
  expiresInSeconds?: number;
  /** Extra claims merged into the payload. */
  claims?: Record<string, unknown>;
}

function b64url(obj: object): string {
  return Buffer.from(JSON.stringify(obj)).toString('base64url');
}

/**
 * Generate an unsigned ID token compatible with the Firebase Auth Emulator.
 * Returns a string of the form `<header>.<payload>.` (empty signature).
 */
export function generateTestIdToken(opts: TestIdTokenOptions): string {
  const now = Math.floor(Date.now() / 1000);
  const expiresIn = opts.expiresInSeconds ?? 3600;

  const header = { alg: 'none', typ: 'JWT' };
  const payload = {
    iss: `https://securetoken.google.com/${opts.projectId}`,
    aud: opts.projectId,
    sub: opts.uid,
    uid: opts.uid,
    email: opts.email,
    iat: now,
    exp: now + expiresIn,
    auth_time: now,
    firebase: {
      identities: {},
      sign_in_provider: 'custom',
    },
    ...opts.claims,
  };

  return `${b64url(header)}.${b64url(payload)}.`;
}
