/**
 * E2E tests for Firebase Auth getUser / getUserByEmail (Identity Toolkit accounts:lookup).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp } from '../_setup';

describe('Firebase Auth getUser (e2e)', () => {
  let auth: admin.auth.Auth;

  before(function () {
    auth = getAdminApp().auth();
  });

  it('getUserByEmail returns user after createUser', async function () {
    const email = `lookup-email-${Date.now()}@example.com`;
    const created = await auth.createUser({
      email,
      displayName: 'Lookup Test',
    });
    const user = await auth.getUserByEmail(email);
    expect(user.uid).to.equal(created.uid);
    expect(user.email).to.equal(email);
    expect(user.displayName).to.equal('Lookup Test');
  });

  it('getUser returns user by uid', async function () {
    const email = `lookup-uid-${Date.now()}@example.com`;
    const created = await auth.createUser({ email });
    const user = await auth.getUser(created.uid);
    expect(user.uid).to.equal(created.uid);
    expect(user.email).to.equal(email);
  });

  it('getUserByEmail throws for non-existent email', async function () {
    try {
      await auth.getUserByEmail('nonexistent@example.com');
      expect.fail('Expected getUserByEmail to throw');
    } catch (err: unknown) {
      const message = (err as Error)?.message ?? String(err);
      expect(message).to.match(/user|not found|exists/i);
    }
  });

  it('getUser throws for non-existent uid', async function () {
    try {
      await auth.getUser('non-existent-uid-12345');
      expect.fail('Expected getUser to throw');
    } catch (err: unknown) {
      const message = (err as Error)?.message ?? String(err);
      expect(message).to.match(/user|not found|exists/i);
    }
  });
});
