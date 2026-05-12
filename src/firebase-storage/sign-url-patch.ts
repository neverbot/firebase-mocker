/**
 * Monkey-patches @google-cloud/storage's File.prototype.getSignedUrl to bypass
 * real cryptographic signing (which requires service account credentials).
 *
 * In emulator mode, returns a URL pointing to the emulator's existing
 * GET /b/:bucket/o/:file?alt=media route. The URL works without any signature
 * because the emulator does not validate signatures.
 */

import { getLogger } from '../logger';

type GetSignedUrlFn = (this: {
  bucket: { name: string };
  name: string;
}) => Promise<[string]>;

let originalGetSignedUrl: GetSignedUrlFn | null = null;

export function patchGetSignedUrl(host: string, port: number): void {
  const logger = getLogger();
  try {
    // Lazy require so the patch is a no-op if @google-cloud/storage isn't installed

    const storageModule = require('@google-cloud/storage') as {
      File: { prototype: { getSignedUrl: GetSignedUrlFn } };
    };

    if (!originalGetSignedUrl) {
      originalGetSignedUrl = storageModule.File.prototype.getSignedUrl;
    }

    storageModule.File.prototype.getSignedUrl = async function (): Promise<
      [string]
    > {
      const bucketName = this.bucket.name;
      const filename = encodeURIComponent(this.name);
      const url =
        `http://${host}:${port}/b/${bucketName}/o/${filename}` +
        `?alt=media&x-goog-signature=emulator-fake-signature`;
      return Promise.resolve([url]);
    };

    logger.info(
      'storage',
      `[STORAGE] Patched @google-cloud/storage File.getSignedUrl to use emulator at ${host}:${port}`,
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    logger.warn(
      'storage',
      `[STORAGE] Could not patch getSignedUrl (@google-cloud/storage missing?): ${message}`,
    );
  }
}

export function unpatchGetSignedUrl(): void {
  if (!originalGetSignedUrl) {
    return;
  }
  try {
    const storageModule = require('@google-cloud/storage') as {
      File: { prototype: { getSignedUrl: GetSignedUrlFn } };
    };
    storageModule.File.prototype.getSignedUrl = originalGetSignedUrl;
    originalGetSignedUrl = null;
  } catch {
    // Module not available; nothing to restore.
  }
}
