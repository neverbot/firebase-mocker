/**
 * E2E tests for accounts:signInWithCustomToken — exchanges a custom token
 * produced by `auth.createCustomToken()` for an idToken accepted by
 * `auth.verifyIdToken()`.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp } from '../_setup';

const AUTH_PORT = 9099;
const PROJECT_ID = 'test-project';

async function signInWithCustomToken(token: string): Promise<{
  idToken: string;
  localId: string;
  isNewUser: boolean;
  refreshToken: string;
}> {
  const res = await fetch(
    `http://localhost:${AUTH_PORT}/identitytoolkit.googleapis.com/v1/projects/${PROJECT_ID}/accounts:signInWithCustomToken`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token, returnSecureToken: true }),
    },
  );
  if (!res.ok) {
    throw new Error(
      `signInWithCustomToken HTTP ${res.status}: ${await res.text()}`,
    );
  }
  return (await res.json()) as {
    idToken: string;
    localId: string;
    isNewUser: boolean;
    refreshToken: string;
  };
}

describe('Auth signInWithCustomToken (e2e)', () => {
  let auth: admin.auth.Auth;
  const ts = Date.now();

  before(function () {
    auth = getAdminApp().auth();
  });

  it('returns an idToken that verifyIdToken accepts', async function () {
    const uid = `cst-uid-${ts}`;
    await auth.createUser({ uid });

    const customToken = await auth.createCustomToken(uid);
    const res = await signInWithCustomToken(customToken);

    expect(res.idToken).to.be.a('string');
    expect(res.localId).to.equal(uid);
    expect(res.isNewUser).to.equal(false);

    const decoded = await auth.verifyIdToken(res.idToken);
    expect(decoded.uid).to.equal(uid);
    expect(decoded.aud).to.equal(PROJECT_ID);
  });

  it('auto-creates the user when the uid does not exist', async function () {
    const uid = `cst-new-${ts}`;
    const customToken = await auth.createCustomToken(uid);
    const res = await signInWithCustomToken(customToken);

    expect(res.localId).to.equal(uid);
    expect(res.isNewUser).to.equal(true);

    const created = await auth.getUser(uid);
    expect(created.uid).to.equal(uid);
  });

  it('preserves custom claims from the custom token', async function () {
    const uid = `cst-claims-${ts}`;
    await auth.createUser({ uid });
    const customToken = await auth.createCustomToken(uid, {
      role: 'editor',
    });
    const res = await signInWithCustomToken(customToken);

    const decoded = (await auth.verifyIdToken(
      res.idToken,
    )) as admin.auth.DecodedIdToken & { role?: string };
    expect(decoded.role).to.equal('editor');
  });

  it('returns 400 for a missing token', async function () {
    const res = await fetch(
      `http://localhost:${AUTH_PORT}/identitytoolkit.googleapis.com/v1/projects/${PROJECT_ID}/accounts:signInWithCustomToken`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      },
    );
    expect(res.status).to.equal(400);
  });

  it('returns 400 for an unparseable token', async function () {
    const res = await fetch(
      `http://localhost:${AUTH_PORT}/identitytoolkit.googleapis.com/v1/projects/${PROJECT_ID}/accounts:signInWithCustomToken`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: 'not-a-jwt' }),
      },
    );
    expect(res.status).to.equal(400);
  });
});
