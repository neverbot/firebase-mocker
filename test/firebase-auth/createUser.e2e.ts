/**
 * E2E tests for Firebase Auth createUser (Identity Toolkit accounts endpoint).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp, getAuthStorage } from '../_setup';

describe('Firebase Auth createUser (e2e)', () => {
  let auth: admin.auth.Auth;

  before(function () {
    auth = getAdminApp().auth();
  });

  it('creates a user with email and returns uid', async function () {
    const email = `create-${Date.now()}@example.com`;
    const userRecord = await auth.createUser({
      email,
      password: 'password123',
      displayName: 'Test User',
    });
    expect(userRecord.uid).to.be.a('string');
    expect(userRecord.email).to.equal(email);
    expect(userRecord.displayName).to.equal('Test User');
  });

  it('created user is in auth storage', async function () {
    const email = `storage-${Date.now()}@example.com`;
    const userRecord = await auth.createUser({ email });
    const storage = getAuthStorage();
    const byUid = storage.getByUid(userRecord.uid);
    expect(byUid).to.exist;
    expect(byUid?.email).to.equal(email);
  });

  it('createUser with existing email throws', async function () {
    const email = `dupe-${Date.now()}@example.com`;
    await auth.createUser({ email });
    try {
      await auth.createUser({ email });
      expect.fail('Expected createUser to throw');
    } catch (err: unknown) {
      const message = (err as Error)?.message ?? String(err);
      expect(message).to.include('already in use');
    }
  });
});
