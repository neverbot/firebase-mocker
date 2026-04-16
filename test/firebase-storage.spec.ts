/**
 * Unit tests for Firebase Storage emulator: StorageStorage public API.
 * HTTP handlers are covered by the e2e tests in test/firebase-storage/*.e2e.ts.
 */

import { expect } from 'chai';
import { StorageStorage } from '../src/firebase-storage';

describe('Firebase Storage (unit)', () => {
  describe('StorageStorage', () => {
    let storage: StorageStorage;

    beforeEach(() => {
      storage = new StorageStorage();
    });

    it('setObject and getObject return stored object', () => {
      const data = Buffer.from('hello world');
      storage.setObject('bucket', 'file.txt', data, 'text/plain');

      const obj = storage.getObject('bucket', 'file.txt');
      expect(obj).to.exist;
      expect(obj!.data.toString()).to.equal('hello world');
      expect(obj!.metadata.contentType).to.equal('text/plain');
      expect(obj!.metadata.size).to.equal(11);
      expect(obj!.metadata.bucket).to.equal('bucket');
      expect(obj!.metadata.name).to.equal('file.txt');
    });

    it('getObject returns undefined for missing object', () => {
      const obj = storage.getObject('bucket', 'nope.txt');
      expect(obj).to.be.undefined;
    });

    it('deleteObject removes the object', () => {
      storage.setObject(
        'bucket',
        'file.txt',
        Buffer.from('data'),
        'text/plain',
      );
      const deleted = storage.deleteObject('bucket', 'file.txt');
      expect(deleted).to.be.true;
      expect(storage.getObject('bucket', 'file.txt')).to.be.undefined;
    });

    it('deleteObject returns false for missing object', () => {
      expect(storage.deleteObject('bucket', 'nope.txt')).to.be.false;
    });

    it('listObjects returns all objects in bucket', () => {
      storage.setObject('bucket', 'a.txt', Buffer.from('a'), 'text/plain');
      storage.setObject('bucket', 'b.txt', Buffer.from('b'), 'text/plain');

      const result = storage.listObjects('bucket');
      expect(result.items).to.have.length(2);
      expect(result.items.map((i) => i.name)).to.deep.equal(['a.txt', 'b.txt']);
    });

    it('listObjects filters by prefix', () => {
      storage.setObject('bucket', 'dir/a.txt', Buffer.from('a'), 'text/plain');
      storage.setObject('bucket', 'dir/b.txt', Buffer.from('b'), 'text/plain');
      storage.setObject('bucket', 'other.txt', Buffer.from('c'), 'text/plain');

      const result = storage.listObjects('bucket', 'dir/');
      expect(result.items).to.have.length(2);
    });

    it('listObjects supports delimiter for prefix grouping', () => {
      storage.setObject('bucket', 'dir/a.txt', Buffer.from('a'), 'text/plain');
      storage.setObject('bucket', 'dir/b.txt', Buffer.from('b'), 'text/plain');
      storage.setObject('bucket', 'root.txt', Buffer.from('c'), 'text/plain');

      const result = storage.listObjects('bucket', undefined, '/');
      expect(result.items).to.have.length(1);
      expect(result.items[0].name).to.equal('root.txt');
      expect(result.prefixes).to.deep.equal(['dir/']);
    });

    it('updateMetadata patches object metadata', () => {
      storage.setObject(
        'bucket',
        'file.txt',
        Buffer.from('data'),
        'text/plain',
      );

      const updated = storage.updateMetadata('bucket', 'file.txt', {
        contentType: 'application/json',
      });
      expect(updated).to.exist;
      expect(updated!.contentType).to.equal('application/json');
      expect(Number(updated!.metageneration)).to.equal(2);
    });

    it('resumable upload flow works', () => {
      storage.createPendingUpload(
        'session-1',
        'bucket',
        'upload.txt',
        'text/plain',
      );
      const pending = storage.getPendingUpload('session-1');
      expect(pending).to.exist;
      expect(pending!.name).to.equal('upload.txt');

      const data = Buffer.from('uploaded content');
      const metadata = storage.completePendingUpload('session-1', data);
      expect(metadata).to.exist;
      expect(metadata!.size).to.equal(data.length);

      const obj = storage.getObject('bucket', 'upload.txt');
      expect(obj).to.exist;
      expect(obj!.data.toString()).to.equal('uploaded content');
    });

    it('clear removes everything', () => {
      storage.setObject(
        'bucket',
        'file.txt',
        Buffer.from('data'),
        'text/plain',
      );
      storage.createPendingUpload(
        'session-1',
        'bucket',
        'up.txt',
        'text/plain',
      );

      storage.clear();
      expect(storage.getObject('bucket', 'file.txt')).to.be.undefined;
      expect(storage.getPendingUpload('session-1')).to.be.undefined;
    });

    it('stores custom metadata', () => {
      storage.setObject(
        'bucket',
        'file.txt',
        Buffer.from('data'),
        'text/plain',
        {
          customKey: 'customValue',
        },
      );
      const obj = storage.getObject('bucket', 'file.txt');
      expect(obj!.metadata.metadata).to.deep.equal({
        customKey: 'customValue',
      });
    });
  });
});
