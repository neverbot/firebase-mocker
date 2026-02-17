/**
 * E2E tests for Firebase Auth deleteUser (Identity Toolkit accounts:delete).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp, getAuthStorage } from '../_setup';

describe('Firebase Auth deleteUser (e2e)', () => {
  let auth: admin.auth.Auth;

  before(function () {
    auth = getAdminApp().auth();
  });

  it('deleteUser removes user; getUser throws', async function () {
    const email = `delete-${Date.now()}@example.com`;
    const created = await auth.createUser({ email });
    await auth.deleteUser(created.uid);
    try {
      await auth.getUser(created.uid);
      expect.fail('Expected getUser to throw after delete');
    } catch {
      // expected
    }
  });

  it('deleted user is removed from auth storage', async function () {
    const email = `delete-storage-${Date.now()}@example.com`;
    const created = await auth.createUser({ email });
    const storage = getAuthStorage();
    expect(storage.getByUid(created.uid)).to.exist;
    await auth.deleteUser(created.uid);
    expect(storage.getByUid(created.uid)).to.be.undefined;
  });

  it('deleteUser on non-existent uid does not throw', async function () {
    await auth.deleteUser('non-existent-uid-99999');
  });
});
