/**
 * In-memory storage for Firebase Storage emulator.
 * Stores file data (Buffer) and metadata keyed by bucket and object path.
 */

import crypto from 'crypto';
import { getLogger } from '../logger';

export interface StoredObjectMetadata {
  name: string;
  bucket: string;
  contentType: string;
  size: number;
  timeCreated: string;
  updated: string;
  generation: string;
  metageneration: string;
  md5Hash: string;
  crc32c: string;
  etag: string;
  storageClass: string;
  contentEncoding?: string;
  contentDisposition?: string;
  cacheControl?: string;
  metadata?: Record<string, string>;
  downloadTokens?: string;
}

export interface StoredObject {
  data: Buffer;
  metadata: StoredObjectMetadata;
}

export interface PendingUpload {
  bucket: string;
  name: string;
  contentType: string;
  metadata?: Record<string, string>;
  chunks: Buffer[];
}

export class StorageStorage {
  private readonly buckets = new Map<string, Map<string, StoredObject>>();
  private readonly pendingUploads = new Map<string, PendingUpload>();
  private readonly logger = getLogger();
  private generationCounter = 1;

  private getBucket(bucketName: string): Map<string, StoredObject> {
    let bucket = this.buckets.get(bucketName);
    if (!bucket) {
      bucket = new Map();
      this.buckets.set(bucketName, bucket);
    }
    return bucket;
  }

  getBuckets(): Map<string, Map<string, StoredObject>> {
    return this.buckets;
  }

  getObject(bucketName: string, objectName: string): StoredObject | undefined {
    return this.buckets.get(bucketName)?.get(objectName);
  }

  setObject(
    bucketName: string,
    objectName: string,
    data: Buffer,
    contentType: string,
    customMetadata?: Record<string, string>,
  ): StoredObjectMetadata {
    const bucket = this.getBucket(bucketName);
    const now = new Date().toISOString();
    const generation = String(this.generationCounter++);
    const token = this.generateToken();

    const metadata: StoredObjectMetadata = {
      name: objectName,
      bucket: bucketName,
      contentType,
      size: data.length,
      timeCreated: now,
      updated: now,
      generation,
      metageneration: '1',
      md5Hash: this.computeMd5(data),
      crc32c: this.computeCrc32c(data),
      etag: `"${generation}"`,
      storageClass: 'STANDARD',
      downloadTokens: token,
    };

    if (customMetadata && Object.keys(customMetadata).length > 0) {
      metadata.metadata = customMetadata;
    }

    bucket.set(objectName, { data, metadata });
    return metadata;
  }

  deleteObject(bucketName: string, objectName: string): boolean {
    const bucket = this.buckets.get(bucketName);
    if (!bucket) {
      return false;
    }
    return bucket.delete(objectName);
  }

  listObjects(
    bucketName: string,
    prefix?: string,
    delimiter?: string,
    maxResults?: number,
    pageToken?: string,
  ): {
    items: StoredObjectMetadata[];
    prefixes: string[];
    nextPageToken?: string;
  } {
    const bucket = this.buckets.get(bucketName);
    if (!bucket) {
      return { items: [], prefixes: [] };
    }

    let entries = Array.from(bucket.values());

    if (prefix) {
      entries = entries.filter((obj) => obj.metadata.name.startsWith(prefix));
    }

    const prefixes: string[] = [];
    if (delimiter) {
      const prefixSet = new Set<string>();
      entries = entries.filter((obj) => {
        const nameAfterPrefix = prefix
          ? obj.metadata.name.slice(prefix.length)
          : obj.metadata.name;
        const delimiterIndex = nameAfterPrefix.indexOf(delimiter);
        if (delimiterIndex >= 0) {
          const commonPrefix =
            (prefix || '') +
            nameAfterPrefix.slice(0, delimiterIndex + delimiter.length);
          prefixSet.add(commonPrefix);
          return false;
        }
        return true;
      });
      prefixes.push(...Array.from(prefixSet).sort());
    }

    entries.sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));

    let startIndex = 0;
    if (pageToken) {
      startIndex = entries.findIndex((obj) => obj.metadata.name > pageToken);
      if (startIndex < 0) {
        startIndex = entries.length;
      }
    }

    const limit = maxResults || entries.length;
    const sliced = entries.slice(startIndex, startIndex + limit);
    const nextPageToken =
      startIndex + limit < entries.length
        ? sliced[sliced.length - 1].metadata.name
        : undefined;

    return {
      items: sliced.map((obj) => obj.metadata),
      prefixes,
      nextPageToken,
    };
  }

  updateMetadata(
    bucketName: string,
    objectName: string,
    patch: Partial<StoredObjectMetadata>,
  ): StoredObjectMetadata | undefined {
    const obj = this.getObject(bucketName, objectName);
    if (!obj) {
      return undefined;
    }

    const now = new Date().toISOString();
    const newMetageneration = String(Number(obj.metadata.metageneration) + 1);

    Object.assign(obj.metadata, patch, {
      updated: now,
      metageneration: newMetageneration,
    });

    return obj.metadata;
  }

  createPendingUpload(
    sessionId: string,
    bucket: string,
    name: string,
    contentType: string,
    metadata?: Record<string, string>,
  ): void {
    this.pendingUploads.set(sessionId, {
      bucket,
      name,
      contentType,
      metadata,
      chunks: [],
    });
  }

  getPendingUpload(sessionId: string): PendingUpload | undefined {
    return this.pendingUploads.get(sessionId);
  }

  completePendingUpload(
    sessionId: string,
    data: Buffer,
  ): StoredObjectMetadata | undefined {
    const upload = this.pendingUploads.get(sessionId);
    if (!upload) {
      return undefined;
    }
    this.pendingUploads.delete(sessionId);
    return this.setObject(
      upload.bucket,
      upload.name,
      data,
      upload.contentType,
      upload.metadata,
    );
  }

  deletePendingUpload(sessionId: string): void {
    this.pendingUploads.delete(sessionId);
  }

  clear(): void {
    this.buckets.clear();
    this.pendingUploads.clear();
  }

  debugLog(): void {
    if (this.buckets.size === 0) {
      this.logger.info('debugStorage', '[STORAGE] No objects');
      return;
    }
    this.logger.info('debugStorage', '[STORAGE] === Objects ===');
    for (const [bucketName, bucket] of this.buckets.entries()) {
      this.logger.info(
        'debugStorage',
        `  Bucket: ${bucketName} (${bucket.size} objects)`,
      );
      for (const [name, obj] of bucket.entries()) {
        this.logger.info(
          'debugStorage',
          `    ${name}: ${obj.metadata.contentType} (${obj.metadata.size} bytes)`,
        );
      }
    }
    this.logger.info('debugStorage', '[STORAGE] === End ===');
  }

  private computeMd5(data: Buffer): string {
    return crypto.createHash('md5').update(data).digest('base64');
  }

  private computeCrc32c(data: Buffer): string {
    // CRC32C (Castagnoli) lookup table
    const table = new Uint32Array(256);
    for (let i = 0; i < 256; i++) {
      let crc = i;
      for (let j = 0; j < 8; j++) {
        crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
      }
      table[i] = crc;
    }

    let crc = 0xffffffff;
    for (const byte of data) {
      crc = table[(crc ^ byte) & 0xff] ^ (crc >>> 8);
    }
    crc = (crc ^ 0xffffffff) >>> 0;

    const buf = Buffer.alloc(4);
    buf.writeUInt32BE(crc, 0);
    return buf.toString('base64');
  }

  private generateToken(): string {
    return crypto.randomUUID();
  }
}
