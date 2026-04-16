/**
 * E2E tests for Firebase Storage emulator using the firebase-admin SDK.
 * Tests upload, download, delete, list, and metadata operations.
 */

import * as fs from 'fs';
import * as http from 'http';
import * as path from 'path';
import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp, getStorageServer, getStorageStorage } from '../_setup';

describe('Firebase Storage (e2e)', () => {
  let bucket: ReturnType<admin.storage.Storage['bucket']>;

  before(function () {
    bucket = getAdminApp().storage().bucket('test-bucket');
  });

  afterEach(function () {
    getStorageStorage().clear();
  });

  describe('upload and download', () => {
    it('uploads a text file and downloads it back', async function () {
      const file = bucket.file('test/hello.txt');
      const content = 'Hello, Firebase Storage Emulator!';

      await file.save(Buffer.from(content), {
        contentType: 'text/plain',
      });

      const [downloaded] = await file.download();
      expect(downloaded.toString()).to.equal(content);
    });

    it('uploads a binary file (PNG) and downloads it intact', async function () {
      const imagePath = path.join(__dirname, 'fixtures', 'test-image.png');
      const imageData = fs.readFileSync(imagePath);

      const file = bucket.file('images/test-image.png');
      await file.save(imageData, {
        contentType: 'image/png',
      });

      const [downloaded] = await file.download();
      expect(Buffer.compare(downloaded, imageData)).to.equal(0);
    });
  });

  describe('metadata', () => {
    it('returns correct metadata after upload', async function () {
      const file = bucket.file('meta-test.txt');
      await file.save(Buffer.from('metadata test'), {
        contentType: 'text/plain',
      });

      const [metadata] = await file.getMetadata();
      expect(metadata.name).to.equal('meta-test.txt');
      expect(metadata.contentType).to.equal('text/plain');
      expect(Number(metadata.size)).to.equal(13);
    });

    it('supports custom metadata', async function () {
      const file = bucket.file('custom-meta.txt');
      await file.save(Buffer.from('data'), {
        contentType: 'text/plain',
        metadata: {
          metadata: {
            customKey: 'customValue',
          },
        },
      });

      const [metadata] = await file.getMetadata();
      expect(metadata.metadata).to.deep.include({
        customKey: 'customValue',
      });
    });
  });

  describe('delete', () => {
    it('deletes an uploaded file', async function () {
      const file = bucket.file('to-delete.txt');
      await file.save(Buffer.from('delete me'), {
        contentType: 'text/plain',
      });

      await file.delete();

      try {
        await file.download();
        expect.fail('Expected download to fail after delete');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message).to.include('404');
      }
    });
  });

  describe('update metadata', () => {
    it('updates content type via setMetadata', async function () {
      const file = bucket.file('update-meta.txt');
      await file.save(Buffer.from('data'), { contentType: 'text/plain' });

      await file.setMetadata({ contentType: 'application/json' });

      const [metadata] = await file.getMetadata();
      expect(metadata.contentType).to.equal('application/json');
    });
  });

  describe('error paths', () => {
    it('download non-existent file returns 404', async function () {
      const file = bucket.file('does-not-exist.txt');
      try {
        await file.download();
        expect.fail('Expected download to fail');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message).to.include('404');
      }
    });

    it('getMetadata on non-existent file throws', async function () {
      const file = bucket.file('no-such-file.txt');
      try {
        await file.getMetadata();
        expect.fail('Expected getMetadata to fail');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message).to.include('No such object');
      }
    });

    it('delete non-existent file throws', async function () {
      const file = bucket.file('ghost.txt');
      try {
        await file.delete();
        expect.fail('Expected delete to fail');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message).to.include('No such object');
      }
    });

    it('setMetadata on non-existent file throws', async function () {
      const file = bucket.file('phantom.txt');
      try {
        await file.setMetadata({ contentType: 'text/html' });
        expect.fail('Expected setMetadata to fail');
      } catch (err: unknown) {
        const message = (err as Error)?.message ?? String(err);
        expect(message).to.include('No such object');
      }
    });
  });

  describe('server helpers', () => {
    it('getConfig returns config', function () {
      const config = getStorageServer().getConfig();
      expect(config.port).to.equal(9199);
      expect(config.host).to.equal('localhost');
    });

    it('debugLog does not throw', function () {
      getStorageServer().debugLog();
    });

    it('debugLog with data does not throw', async function () {
      await bucket
        .file('debug.txt')
        .save(Buffer.from('x'), { contentType: 'text/plain' });
      getStorageServer().debugLog();
    });
  });

  describe('direct HTTP requests', () => {
    async function httpRequest(
      method: string,
      urlPath: string,
      body?: Buffer | string,
    ): Promise<{ status: number; body: string }> {
      return new Promise((resolve, reject) => {
        const req = http.request(
          {
            hostname: 'localhost',
            port: 9199,
            path: urlPath,
            method,
            headers: body
              ? {
                  'Content-Type': 'application/json',
                  'Content-Length': Buffer.byteLength(body),
                }
              : {},
          },
          (res) => {
            const chunks: Buffer[] = [];
            res.on('data', (c: Buffer) => chunks.push(c));
            res.on('end', () => {
              resolve({
                status: res.statusCode || 0,
                body: Buffer.concat(chunks).toString(),
              });
            });
          },
        );
        req.on('error', reject);
        if (body) {
          req.write(body);
        }
        req.end();
      });
    }

    it('catch-all returns 404 for unknown path', async function () {
      const res = await httpRequest('GET', '/unknown/path');
      expect(res.status).to.equal(404);
    });

    it('firebase download /v0/... returns file data', async function () {
      await bucket
        .file('fb-dl.txt')
        .save(Buffer.from('firebase download'), { contentType: 'text/plain' });
      const res = await httpRequest('GET', '/v0/b/test-bucket/o/fb-dl.txt');
      expect(res.status).to.equal(200);
      expect(res.body).to.equal('firebase download');
    });

    it('firebase download /v0/... returns 404 for missing file', async function () {
      const res = await httpRequest('GET', '/v0/b/test-bucket/o/nope.txt');
      expect(res.status).to.equal(404);
    });

    it('list on empty bucket returns empty', async function () {
      const res = await httpRequest('GET', '/b/empty-bucket/o');
      expect(res.status).to.equal(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.items).to.be.undefined;
    });
  });

  describe('list', () => {
    it('lists files in bucket', async function () {
      await bucket
        .file('list/a.txt')
        .save(Buffer.from('a'), { contentType: 'text/plain' });
      await bucket
        .file('list/b.txt')
        .save(Buffer.from('b'), { contentType: 'text/plain' });
      await bucket
        .file('other.txt')
        .save(Buffer.from('c'), { contentType: 'text/plain' });

      const [files] = await bucket.getFiles({ prefix: 'list/' });
      expect(files).to.have.length(2);
      const names = files.map((f) => f.name);
      expect(names).to.include('list/a.txt');
      expect(names).to.include('list/b.txt');
    });

    it('lists all files without prefix', async function () {
      await bucket
        .file('one.txt')
        .save(Buffer.from('1'), { contentType: 'text/plain' });
      await bucket
        .file('two.txt')
        .save(Buffer.from('2'), { contentType: 'text/plain' });

      const [files] = await bucket.getFiles();
      expect(files).to.have.length(2);
    });
  });
});
