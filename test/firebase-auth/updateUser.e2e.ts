/**
 * E2E tests for Firebase Auth updateUser (Identity Toolkit accounts:update).
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp } from '../_setup';

describe('Firebase Auth updateUser (e2e)', () => {
  let auth: admin.auth.Auth;

  before(function () {
    auth = getAdminApp().auth();
  });

  it('updateUser changes displayName', async function () {
    const email = `update-${Date.now()}@example.com`;
    const created = await auth.createUser({
      email,
      displayName: 'Original',
    });
    await auth.updateUser(created.uid, { displayName: 'Updated' });
    const user = await auth.getUser(created.uid);
    expect(user.displayName).to.equal('Updated');
  });

  it('updateUser changes email', async function () {
    const email1 = `update-email1-${Date.now()}@example.com`;
    const email2 = `update-email2-${Date.now()}@example.com`;
    const created = await auth.createUser({ email: email1 });
    await auth.updateUser(created.uid, { email: email2 });
    const user = await auth.getUser(created.uid);
    expect(user.email).to.equal(email2);
    const byEmail = await auth.getUserByEmail(email2);
    expect(byEmail.uid).to.equal(created.uid);
  });

  it('updateUser on non-existent uid throws', async function () {
    try {
      await auth.updateUser('non-existent-uid-99999', {
        displayName: 'Fail',
      });
      expect.fail('Expected updateUser to throw');
    } catch (err: unknown) {
      const message = (err as Error)?.message ?? String(err);
      expect(message).to.match(/user|not found|exists/i);
    }
  });
});
