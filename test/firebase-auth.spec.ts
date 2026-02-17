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
