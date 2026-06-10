/**
 * E2E tests for Firebase Auth generatePasswordResetLink (accounts:sendOobCode).
 */

import { expect } from 'chai';
import { Auth, getAuth } from 'firebase-admin/auth';
import { getAdminApp, getAuthStorage } from '../_setup';

describe('Firebase Auth generatePasswordResetLink (e2e)', () => {
  let auth: Auth;

  before(function () {
    auth = getAuth(getAdminApp());
  });

  it('generatePasswordResetLink returns a URL with oobCode', async function () {
    const email = `reset-${Date.now()}@example.com`;
    const link = await auth.generatePasswordResetLink(email);
    expect(link).to.be.a('string');
    const url = new URL(link);
    const oobCode = url.searchParams.get('oobCode');
    expect(oobCode).to.be.a('string').and.not.empty;
  });

  it('oobCode is stored in auth storage with PASSWORD_RESET type', async function () {
    const email = `stored-${Date.now()}@example.com`;
    const link = await auth.generatePasswordResetLink(email);
    const oobCode = new URL(link).searchParams.get('oobCode')!;
    const entry = getAuthStorage().getOobCode(oobCode);
    expect(entry).to.exist;
    expect(entry?.email).to.equal(email);
    expect(entry?.requestType).to.equal('PASSWORD_RESET');
  });

  it('multiple calls produce different oobCodes', async function () {
    const email = `multi-${Date.now()}@example.com`;
    const link1 = await auth.generatePasswordResetLink(email);
    const link2 = await auth.generatePasswordResetLink(email);
    const code1 = new URL(link1).searchParams.get('oobCode');
    const code2 = new URL(link2).searchParams.get('oobCode');
    expect(code1).to.not.equal(code2);
  });

  it('oobLink uses resetPassword mode', async function () {
    const link = await auth.generatePasswordResetLink('mode@example.com');
    const url = new URL(link);
    expect(url.searchParams.get('mode')).to.equal('resetPassword');
  });
});
