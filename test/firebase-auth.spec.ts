/**
 * Unit tests for Firebase Auth emulator: AuthStorage and AuthServer public API.
 * Auth HTTP handlers are covered by the e2e tests in test/firebase-auth/*.e2e.ts.
 */

import { expect } from 'chai';
import { AuthStorage, AuthEmulatorUser } from '../src/firebase-auth';
import { getAuthServer } from './_setup';

describe('Firebase Auth (unit)', () => {
  describe('AuthStorage', () => {
    let storage: AuthStorage;

    beforeEach(() => {
      storage = new AuthStorage();
    });

    it('add and getByUid return user', () => {
      const user: AuthEmulatorUser = {
        localId: 'uid1',
        email: 'u1@example.com',
        displayName: 'User One',
        createdAt: new Date().toISOString(),
      };
      storage.add(user);
      const found = storage.getByUid('uid1');
      expect(found).to.deep.include({
        localId: 'uid1',
        email: 'u1@example.com',
      });
    });

    it('getByEmail finds user (case-insensitive)', () => {
      const user: AuthEmulatorUser = {
        localId: 'uid2',
        email: 'U2@Example.com',
        createdAt: new Date().toISOString(),
      };
      storage.add(user);
      expect(storage.getByEmail('u2@example.com')).to.exist;
      expect(storage.getByEmail('U2@Example.com')?.localId).to.equal('uid2');
    });

    it('getByUid returns undefined for unknown uid', () => {
      expect(storage.getByUid('unknown')).to.be.undefined;
    });

    it('getByEmail returns undefined for unknown email', () => {
      expect(storage.getByEmail('unknown@example.com')).to.be.undefined;
    });

    it('deleteByUid removes user', () => {
      const user: AuthEmulatorUser = {
        localId: 'uid3',
        email: 'u3@example.com',
        createdAt: new Date().toISOString(),
      };
      storage.add(user);
      expect(storage.deleteByUid('uid3')).to.be.true;
      expect(storage.getByUid('uid3')).to.be.undefined;
      expect(storage.getByEmail('u3@example.com')).to.be.undefined;
    });

    it('deleteByUid returns false for unknown uid', () => {
      expect(storage.deleteByUid('unknown')).to.be.false;
    });

    it('clear removes all users', () => {
      storage.add({
        localId: 'a',
        email: 'a@x.com',
        createdAt: new Date().toISOString(),
      });
      storage.add({
        localId: 'b',
        email: 'b@x.com',
        createdAt: new Date().toISOString(),
      });
      storage.clear();
      expect(storage.listUids()).to.have.length(0);
      expect(storage.getByUid('a')).to.be.undefined;
    });

    it('listUids returns all uids', () => {
      storage.add({
        localId: 'x',
        email: 'x@x.com',
        createdAt: new Date().toISOString(),
      });
      storage.add({
        localId: 'y',
        email: 'y@x.com',
        createdAt: new Date().toISOString(),
      });
      const uids = storage.listUids();
      expect(uids).to.have.members(['x', 'y']);
    });

    it('debugLog does not throw (empty storage)', () => {
      expect(() => storage.debugLog()).to.not.throw();
    });

    it('debugLog does not throw (with users)', () => {
      storage.add({
        localId: 'd',
        email: 'd@x.com',
        createdAt: new Date().toISOString(),
      });
      expect(() => storage.debugLog()).to.not.throw();
    });

    describe('OOB codes', () => {
      it('createOobCode returns a non-empty string and stores entry', () => {
        const code = storage.createOobCode('a@b.com', 'PASSWORD_RESET');
        expect(code).to.be.a('string').and.not.empty;
        const entry = storage.getOobCode(code);
        expect(entry).to.exist;
        expect(entry?.email).to.equal('a@b.com');
        expect(entry?.requestType).to.equal('PASSWORD_RESET');
        expect(entry?.createdAt).to.be.a('string');
      });

      it('createOobCode produces unique codes', () => {
        const c1 = storage.createOobCode('a@b.com', 'PASSWORD_RESET');
        const c2 = storage.createOobCode('a@b.com', 'PASSWORD_RESET');
        expect(c1).to.not.equal(c2);
      });

      it('getOobCode returns undefined for unknown code', () => {
        expect(storage.getOobCode('does-not-exist')).to.be.undefined;
      });

      it('consumeOobCode returns entry and removes it', () => {
        const code = storage.createOobCode('a@b.com', 'PASSWORD_RESET');
        const entry = storage.consumeOobCode(code);
        expect(entry?.email).to.equal('a@b.com');
        expect(storage.getOobCode(code)).to.be.undefined;
      });

      it('consumeOobCode returns undefined for unknown code', () => {
        expect(storage.consumeOobCode('nope')).to.be.undefined;
      });

      it('clear removes oobCodes too', () => {
        const code = storage.createOobCode('a@b.com', 'PASSWORD_RESET');
        storage.clear();
        expect(storage.getOobCode(code)).to.be.undefined;
      });
    });
  });

  describe('generateTestIdToken', () => {
    const { generateTestIdToken } =
      require('../src/firebase-auth/jwt') as typeof import('../src/firebase-auth/jwt');

    it('returns a 3-part dot-separated string with empty signature', () => {
      const token = generateTestIdToken({
        uid: 'u1',
        projectId: 'test-project',
      });
      const parts = token.split('.');
      expect(parts).to.have.length(3);
      expect(parts[2]).to.equal('');
    });

    it('header is {alg: "none", typ: "JWT"}', () => {
      const token = generateTestIdToken({
        uid: 'u1',
        projectId: 'test-project',
      });
      const headerB64 = token.split('.')[0];
      const header = JSON.parse(
        Buffer.from(headerB64, 'base64url').toString('utf8'),
      );
      expect(header).to.deep.equal({ alg: 'none', typ: 'JWT' });
    });

    it('payload has expected claims', () => {
      const token = generateTestIdToken({
        uid: 'u1',
        email: 'a@b.com',
        projectId: 'test-project',
      });
      const payloadB64 = token.split('.')[1];
      const payload = JSON.parse(
        Buffer.from(payloadB64, 'base64url').toString('utf8'),
      );
      expect(payload.iss).to.equal(
        'https://securetoken.google.com/test-project',
      );
      expect(payload.aud).to.equal('test-project');
      expect(payload.sub).to.equal('u1');
      expect(payload.uid).to.equal('u1');
      expect(payload.email).to.equal('a@b.com');
      expect(payload.iat).to.be.a('number');
      expect(payload.exp).to.be.a('number');
      expect(payload.exp).to.be.greaterThan(payload.iat);
      expect(payload.auth_time).to.equal(payload.iat);
      expect(payload.firebase).to.deep.equal({
        identities: {},
        sign_in_provider: 'custom',
      });
    });

    it('expiresInSeconds is respected', () => {
      const token = generateTestIdToken({
        uid: 'u1',
        projectId: 'test-project',
        expiresInSeconds: 60,
      });
      const payload = JSON.parse(
        Buffer.from(token.split('.')[1], 'base64url').toString('utf8'),
      );
      expect(payload.exp - payload.iat).to.equal(60);
    });

    it('custom claims are merged into payload', () => {
      const token = generateTestIdToken({
        uid: 'u1',
        projectId: 'test-project',
        claims: { roles: ['admin'], orgId: 'org-1' },
      });
      const payload = JSON.parse(
        Buffer.from(token.split('.')[1], 'base64url').toString('utf8'),
      );
      expect(payload.roles).to.deep.equal(['admin']);
      expect(payload.orgId).to.equal('org-1');
    });
  });

  describe('AuthServer (via getAuthServer)', () => {
    it('getStorage returns storage', () => {
      const server = getAuthServer();
      const storage = server.getStorage();
      expect(storage).to.exist;
      expect(typeof storage.getByUid).to.equal('function');
      expect(typeof storage.add).to.equal('function');
    });

    it('debugLog does not throw', () => {
      const server = getAuthServer();
      expect(() => server.debugLog()).to.not.throw();
    });
  });
});
