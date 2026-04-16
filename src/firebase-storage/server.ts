/**
 * Express-based HTTP server that emulates the Google Cloud Storage JSON API.
 * firebase-admin Storage (via @google-cloud/storage) uses this when
 * STORAGE_EMULATOR_HOST is set.
 */

import crypto from 'crypto';
import express, { Request, Response } from 'express';
import { getLogger } from '../logger';
import { StorageStorage } from './storage';

export interface StorageServerConfig {
  port: number;
  host: string;
  projectId?: string;
}

export class StorageServer {
  private readonly storage = new StorageStorage();
  private readonly config: StorageServerConfig;
  private server?: ReturnType<express.Application['listen']>;
  private readonly logger = getLogger();
  private readonly app = express();

  constructor(config: StorageServerConfig) {
    this.config = config;
    this.setupRoutes();
  }

  getStorage(): StorageStorage {
    return this.storage;
  }

  getConfig(): Readonly<StorageServerConfig> {
    return { ...this.config };
  }

  private setupRoutes(): void {
    // Resumable upload: create session
    this.app.post(
      '/upload/storage/v1/b/:bucket/o',
      express.json({ limit: '200mb' }),
      (req: Request, res: Response) => {
        this.handleCreateResumableUpload(req, res);
      },
    );

    // Resumable upload: upload data (PUT to same path with upload_id)
    this.app.put(
      '/upload/storage/v1/b/:bucket/o',
      (req: Request, res: Response) => {
        this.handleUploadData(req, res);
      },
    );

    // All /v0/b/:bucket/o/... and /b/:bucket/o/... requests are handled
    // by a single middleware that parses the URL manually, because object
    // names can contain slashes and Express 5 path-to-regexp does not
    // support mixing named params with wildcards easily.
    this.app.use((req: Request, res: Response, next) => {
      // Match /v0/b/{bucket}/o/{objectName...}
      const v0Match = /^\/v0\/b\/([^/]+)\/o\/(.+)$/.exec(req.path);
      if (v0Match && req.method === 'GET') {
        req.params = {
          bucket: v0Match[1],
          objectName: decodeURIComponent(v0Match[2]),
        };
        this.handleFirebaseDownload(req, res);
        return;
      }

      // Match /b/{bucket}/o/{objectName...}
      const objMatch = /^\/b\/([^/]+)\/o\/(.+)$/.exec(req.path);
      if (objMatch) {
        req.params = {
          bucket: objMatch[1],
          objectName: decodeURIComponent(objMatch[2]),
        };
        if (req.method === 'GET') {
          this.handleGetOrDownload(req, res);
        } else if (req.method === 'PATCH') {
          express.json()(req, res, () => {
            this.handleUpdateMetadata(req, res);
          });
        } else if (req.method === 'DELETE') {
          this.handleDeleteObject(req, res);
        } else {
          next();
        }
        return;
      }

      // Match /b/{bucket}/o (list objects, no object name)
      const listMatch = /^\/b\/([^/]+)\/o\/?$/.exec(req.path);
      if (listMatch && req.method === 'GET') {
        req.params = { bucket: listMatch[1] };
        this.handleListObjects(req, res);
        return;
      }

      next();
    });

    // Catch-all
    this.app.use((_req, res) => {
      res.status(404).json({ error: 'Not Found' });
    });

    this.app.use(
      (err: Error, _req: Request, res: Response, _next: () => void) => {
        this.logger.error('error', `[STORAGE] Unhandled error: ${err.message}`);
        res.status(500).json({
          error: { message: err.message, code: 500 },
        });
      },
    );
  }

  private getObjectName(req: Request): string {
    return String(req.params.objectName || '');
  }

  private getBucketName(req: Request): string {
    return String((req.params as Record<string, string>).bucket || '');
  }

  private handleCreateResumableUpload(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const name = req.query.name as string;

    if (!name) {
      res
        .status(400)
        .json({ error: { message: 'Missing object name', code: 400 } });
      return;
    }

    const body = (req.body || {}) as Record<string, unknown>;
    const contentType =
      (body.contentType as string) ||
      (req.headers['x-upload-content-type'] as string) ||
      'application/octet-stream';
    const customMetadata = body.metadata as Record<string, string> | undefined;

    const sessionId = crypto.randomUUID();
    this.storage.createPendingUpload(
      sessionId,
      bucket,
      name,
      contentType,
      customMetadata,
    );

    const baseUrl = `${req.protocol}://${String(req.headers.host)}`;
    const locationUri = `${baseUrl}/upload/storage/v1/b/${bucket}/o?upload_id=${sessionId}&name=${encodeURIComponent(name)}`;

    this.logger.info(
      'storage',
      `[STORAGE] Resumable upload created: bucket=${bucket} name=${name} session=${sessionId}`,
    );

    res.status(200).set('Location', locationUri).json({});
  }

  private handleUploadData(req: Request, res: Response): void {
    const uploadId = req.query.upload_id as string;
    const bucket = this.getBucketName(req);

    if (!uploadId) {
      this.handleSimpleUpload(req, res, bucket);
      return;
    }

    const upload = this.storage.getPendingUpload(uploadId);
    if (!upload) {
      res
        .status(404)
        .json({ error: { message: 'Upload session not found', code: 404 } });
      return;
    }

    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const data = Buffer.concat(chunks);
      this.completeUpload(res, uploadId, bucket, data);
    });
  }

  private completeUpload(
    res: Response,
    uploadId: string,
    bucket: string,
    data: Buffer,
  ): void {
    const metadata = this.storage.completePendingUpload(uploadId, data);
    if (!metadata) {
      res
        .status(500)
        .json({ error: { message: 'Failed to complete upload', code: 500 } });
      return;
    }

    this.logger.info(
      'storage',
      `[STORAGE] Upload completed: bucket=${bucket} name=${metadata.name} size=${metadata.size}`,
    );

    res.status(200).json(metadata);
  }

  private handleSimpleUpload(
    req: Request,
    res: Response,
    bucket: string,
  ): void {
    const name = req.query.name as string;
    if (!name) {
      res
        .status(400)
        .json({ error: { message: 'Missing object name', code: 400 } });
      return;
    }

    const contentType = String(
      req.headers['content-type'] || 'application/octet-stream',
    );

    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const data = Buffer.concat(chunks);
      const metadata = this.storage.setObject(bucket, name, data, contentType);

      this.logger.info(
        'storage',
        `[STORAGE] Simple upload: bucket=${bucket} name=${name} size=${metadata.size}`,
      );

      res.status(200).json(metadata);
    });
  }

  private handleGetOrDownload(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const objectName = this.getObjectName(req);
    const alt = req.query.alt as string;

    const obj = this.storage.getObject(bucket, objectName);
    if (!obj) {
      res.status(404).json({
        error: {
          message: `No such object: ${bucket}/${objectName}`,
          code: 404,
        },
      });
      return;
    }

    if (alt === 'media') {
      this.logger.info(
        'storage',
        `[STORAGE] Download: bucket=${bucket} name=${objectName}`,
      );
      res
        .status(200)
        .set('Content-Type', obj.metadata.contentType)
        .set('Content-Length', String(obj.data.length))
        .send(obj.data);
      return;
    }

    this.logger.info(
      'storage',
      `[STORAGE] Get metadata: bucket=${bucket} name=${objectName}`,
    );
    res.status(200).json(obj.metadata);
  }

  private handleFirebaseDownload(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const objectName = this.getObjectName(req);

    const obj = this.storage.getObject(bucket, objectName);
    if (!obj) {
      res.status(404).json({
        error: {
          message: `No such object: ${bucket}/${objectName}`,
          code: 404,
        },
      });
      return;
    }

    this.logger.info(
      'storage',
      `[STORAGE] Firebase download: bucket=${bucket} name=${objectName}`,
    );
    res
      .status(200)
      .set('Content-Type', obj.metadata.contentType)
      .set('Content-Length', String(obj.data.length))
      .send(obj.data);
  }

  private handleUpdateMetadata(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const objectName = this.getObjectName(req);
    const body = (req.body || {}) as Record<string, unknown>;

    const updated = this.storage.updateMetadata(bucket, objectName, body);
    if (!updated) {
      res.status(404).json({
        error: {
          message: `No such object: ${bucket}/${objectName}`,
          code: 404,
        },
      });
      return;
    }

    this.logger.info(
      'storage',
      `[STORAGE] Update metadata: bucket=${bucket} name=${objectName}`,
    );
    res.status(200).json(updated);
  }

  private handleDeleteObject(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const objectName = this.getObjectName(req);

    const deleted = this.storage.deleteObject(bucket, objectName);
    if (!deleted) {
      res.status(404).json({
        error: {
          message: `No such object: ${bucket}/${objectName}`,
          code: 404,
        },
      });
      return;
    }

    this.logger.info(
      'storage',
      `[STORAGE] Deleted: bucket=${bucket} name=${objectName}`,
    );
    res.status(204).send();
  }

  private handleListObjects(req: Request, res: Response): void {
    const bucket = this.getBucketName(req);
    const prefix = req.query.prefix as string | undefined;
    const delimiter = req.query.delimiter as string | undefined;
    const maxResults = req.query.maxResults
      ? Number(req.query.maxResults)
      : undefined;
    const pageToken = req.query.pageToken as string | undefined;

    const result = this.storage.listObjects(
      bucket,
      prefix,
      delimiter,
      maxResults,
      pageToken,
    );

    this.logger.info(
      'storage',
      `[STORAGE] List objects: bucket=${bucket} prefix=${prefix || '(none)'} count=${result.items.length}`,
    );

    const response: Record<string, unknown> = { kind: 'storage#objects' };
    if (result.items.length > 0) {
      response.items = result.items;
    }
    if (result.prefixes.length > 0) {
      response.prefixes = result.prefixes;
    }
    if (result.nextPageToken) {
      response.nextPageToken = result.nextPageToken;
    }

    res.status(200).json(response);
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.config.port, this.config.host, () => {
        this.logger.info(
          'storage',
          `[STORAGE] Firebase Storage emulator HTTP server running on http://${this.config.host}:${this.config.port}`,
        );
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.server) {
        resolve();
        return;
      }
      this.server.close(() => {
        this.logger.info(
          'storage',
          '[STORAGE] Firebase Storage emulator server stopped',
        );
        this.server = undefined;
        resolve();
      });
    });
  }

  public debugLog(): void {
    this.storage.debugLog();
  }
}
