/**
 * E2E tests for the monkey-patched getSignedUrl in emulator mode.
 */

import * as http from 'http';
import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { getAdminApp, getStorageStorage } from '../_setup';

async function httpGet(
  urlString: string,
): Promise<{ status: number; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const u = new URL(urlString);
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port,
        path: `${u.pathname}${u.search}`,
        method: 'GET',
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          resolve({
            status: res.statusCode || 0,
            body: Buffer.concat(chunks),
          });
        });
      },
    );
    req.on('error', reject);
    req.end();
  });
}

describe('Firebase Storage getSignedUrl (e2e)', () => {
  let bucket: ReturnType<admin.storage.Storage['bucket']>;

  before(function () {
    bucket = getAdminApp().storage().bucket('test-bucket');
  });

  afterEach(function () {
    getStorageStorage().clear();
  });

  it('getSignedUrl returns an array with a URL string', async function () {
    const file = bucket.file('signed/a.txt');
    await file.save(Buffer.from('hello'), { contentType: 'text/plain' });

    const [url] = await file.getSignedUrl({
      action: 'read',
      expires: Date.now() + 60_000,
    });

    expect(url).to.be.a('string');
    expect(url).to.contain('localhost:9199');
    expect(url).to.contain('/b/test-bucket/o/signed%2Fa.txt');
  });

  it('downloading the signed URL returns the file content', async function () {
    const file = bucket.file('signed/b.txt');
    const content = 'signed download body';
    await file.save(Buffer.from(content), { contentType: 'text/plain' });

    const [url] = await file.getSignedUrl({
      action: 'read',
      expires: Date.now() + 60_000,
    });

    const res = await httpGet(url);
    expect(res.status).to.equal(200);
    expect(res.body.toString()).to.equal(content);
  });

  it('getSignedUrl does not require the file to exist', async function () {
    const file = bucket.file('signed/does-not-exist.txt');
    const [url] = await file.getSignedUrl({
      action: 'read',
      expires: Date.now() + 60_000,
    });
    expect(url).to.be.a('string');

    const res = await httpGet(url);
    expect(res.status).to.equal(404);
  });
});
