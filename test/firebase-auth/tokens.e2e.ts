/**
 * E2E tests for createCustomToken and verifyIdToken using firebase-admin
 * in emulator mode, plus the generateTestIdToken helper.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { firebaseMocker } from '../../src/index';
import { getAdminApp } from '../_setup';

describe('Firebase Auth tokens (e2e)', () => {
  let auth: admin.auth.Auth;
  const ts = Date.now();

  before(function () {
    auth = getAdminApp().auth();
  });

  describe('createCustomToken', () => {
    it('returns a JWT-formatted string', async function () {
      const token = await auth.createCustomToken(`uid-1-${ts}`);
      expect(token).to.be.a('string');
      const parts = token.split('.');
      expect(parts).to.have.length(3);
    });

    it('createCustomToken with claims encodes them in the payload', async function () {
      const token = await auth.createCustomToken(`uid-2-${ts}`, {
        role: 'admin',
      });
      const payload = JSON.parse(
        Buffer.from(token.split('.')[1], 'base64url').toString('utf8'),
      );
      expect(payload.uid).to.equal(`uid-2-${ts}`);
      expect(payload.claims).to.deep.equal({ role: 'admin' });
    });
  });

  describe('verifyIdToken with generateTestIdToken', () => {
    // The Firebase Admin SDK in emulator mode always calls getUser(sub) after
    // decoding the token, so the user must exist in the emulator's user store.

    it('verifyIdToken decodes a token produced by generateTestIdToken', async function () {
      await auth.createUser({
        uid: `verify-uid-1-${ts}`,
        email: 'verify@example.com',
      });
      const token = firebaseMocker.generateTestIdToken({
        uid: `verify-uid-1-${ts}`,
        email: 'verify@example.com',
        projectId: 'test-project',
      });
      const decoded = await auth.verifyIdToken(token);
      expect(decoded.uid).to.equal(`verify-uid-1-${ts}`);
      expect(decoded.email).to.equal('verify@example.com');
      expect(decoded.aud).to.equal('test-project');
      expect(decoded.iss).to.equal(
        'https://securetoken.google.com/test-project',
      );
    });

    it('verifyIdToken returns custom claims', async function () {
      await auth.createUser({ uid: `verify-uid-2-${ts}` });
      const token = firebaseMocker.generateTestIdToken({
        uid: `verify-uid-2-${ts}`,
        projectId: 'test-project',
        claims: { roles: ['admin', 'editor'] },
      });
      const decoded = (await auth.verifyIdToken(
        token,
      )) as admin.auth.DecodedIdToken & {
        roles?: string[];
      };
      expect(decoded.roles).to.deep.equal(['admin', 'editor']);
    });

    it('verifyIdToken rejects an expired token', async function () {
      await auth.createUser({ uid: `expired-uid-${ts}` });
      const token = firebaseMocker.generateTestIdToken({
        uid: `expired-uid-${ts}`,
        projectId: 'test-project',
        expiresInSeconds: -60,
      });
      try {
        await auth.verifyIdToken(token);
        expect.fail('Expected verifyIdToken to throw on expired token');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message.toLowerCase()).to.match(/expir|invalid/);
      }
    });
  });
});
