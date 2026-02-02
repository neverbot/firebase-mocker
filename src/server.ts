/**
 * HTTP server that emulates Firestore REST API
 */

import express, { Request, Response } from 'express';
import { FirestoreStorage } from './firestore-storage';
import { ServerConfig } from './types';
import {
  toFirestoreDocument,
  fromFirestoreDocument,
  buildDocumentPath,
  generateDocumentId,
} from './utils';

export class FirestoreServer {
  private readonly app: express.Application;
  private readonly storage: FirestoreStorage;
  private readonly config: ServerConfig;
  private serverInstance?: ReturnType<express.Application['listen']>;

  constructor(config: ServerConfig) {
    this.config = config;
    this.app = express();
    this.storage = new FirestoreStorage();
    this.setupMiddleware();
    this.setupRoutes();
  }

  private setupMiddleware(): void {
    this.app.use(express.json());
    this.app.use((req, res, next) => {
      // eslint-disable-next-line no-console
      console.log(`${req.method} ${req.path}`);
      next();
    });
  }

  private setupRoutes(): void {
    // Get a document
    this.app.get(
      '/v1/projects/:projectId/databases/:databaseId/documents/:collectionId/:docId',
      this.getDocument.bind(this),
    );

    // List documents in a collection
    this.app.get(
      '/v1/projects/:projectId/databases/:databaseId/documents/:collectionId',
      this.listDocuments.bind(this),
    );

    // Create a document with auto-generated ID
    this.app.post(
      '/v1/projects/:projectId/databases/:databaseId/documents/:collectionId',
      this.createDocument.bind(this),
    );

    // Create or update a document (PATCH)
    this.app.patch(
      '/v1/projects/:projectId/databases/:databaseId/documents/:collectionId/:docId',
      this.updateDocument.bind(this),
    );

    // Delete a document
    this.app.delete(
      '/v1/projects/:projectId/databases/:databaseId/documents/:collectionId/:docId',
      this.deleteDocument.bind(this),
    );

    // Health check
    this.app.get('/health', (req, res) => {
      res.json({ status: 'ok' });
    });
  }

  private getDocument(req: Request, res: Response): void {
    try {
      const { projectId, databaseId, collectionId, docId } = req.params;
      const document = this.storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      if (!document) {
        res.status(404).json({
          error: {
            code: 404,
            message: `Document not found: ${collectionId}/${docId}`,
            status: 'NOT_FOUND',
          },
        });
        return;
      }

      res.json(document);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      res.status(500).json({
        error: {
          code: 500,
          message: errorMessage,
          status: 'INTERNAL_ERROR',
        },
      });
    }
  }

  private listDocuments(req: Request, res: Response): void {
    try {
      const { projectId, databaseId, collectionId } = req.params;
      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionId,
      );

      res.json({
        documents,
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      res.status(500).json({
        error: {
          code: 500,
          message: errorMessage,
          status: 'INTERNAL_ERROR',
        },
      });
    }
  }

  private createDocument(req: Request, res: Response): void {
    try {
      const { projectId, databaseId, collectionId } = req.params;
      const docId = generateDocumentId();
      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      // Extract fields from request body
      // The request body should have a 'fields' property with Firestore values
      // For simplicity, we'll also accept a plain object and convert it
      let fields: Record<string, any>;
      if (req.body.fields) {
        // Already in Firestore format
        fields = req.body.fields;
      } else {
        // Plain object - convert to Firestore format
        fields = req.body;
      }

      // Convert plain object to Firestore document format
      const document = toFirestoreDocument(documentPath, fields);
      document.name = documentPath;

      this.storage.setDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
        document,
      );

      res.status(201).json(document);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      res.status(500).json({
        error: {
          code: 500,
          message: errorMessage,
          status: 'INTERNAL_ERROR',
        },
      });
    }
  }

  private updateDocument(req: Request, res: Response): void {
    try {
      const { projectId, databaseId, collectionId, docId } = req.params;
      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      // Get existing document or create new one
      const existingDoc = this.storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      // Extract fields from request body
      let fields: Record<string, any>;
      if (req.body.fields) {
        fields = req.body.fields;
      } else {
        fields = req.body;
      }

      if (existingDoc) {
        // Merge with existing document
        const existingData = fromFirestoreDocument(existingDoc);
        const updateData = Object.keys(fields).reduce(
          (acc, key) => {
            const value = fields[key];
            if (
              typeof value === 'object' &&
              value !== null &&
              'mapValue' in value
            ) {
              acc[key] = fromFirestoreDocument({
                name: '',
                fields: value.mapValue.fields,
              });
            } else {
              acc[key] = value;
            }
            return acc;
          },
          {} as Record<string, any>,
        );

        const mergedData = { ...existingData, ...updateData };
        const updatedDoc = toFirestoreDocument(documentPath, mergedData);
        updatedDoc.name = documentPath;
        updatedDoc.createTime = existingDoc.createTime; // Preserve create time
        updatedDoc.updateTime = new Date().toISOString();

        this.storage.setDocument(
          projectId,
          databaseId,
          collectionId,
          docId,
          updatedDoc,
        );
        res.json(updatedDoc);
      } else {
        // Create new document
        const newDoc = toFirestoreDocument(documentPath, fields);
        newDoc.name = documentPath;
        this.storage.setDocument(
          projectId,
          databaseId,
          collectionId,
          docId,
          newDoc,
        );
        res.status(201).json(newDoc);
      }
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      res.status(500).json({
        error: {
          code: 500,
          message: errorMessage,
          status: 'INTERNAL_ERROR',
        },
      });
    }
  }

  private deleteDocument(req: Request, res: Response): void {
    try {
      const { projectId, databaseId, collectionId, docId } = req.params;
      const deleted = this.storage.deleteDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      if (!deleted) {
        res.status(404).json({
          error: {
            code: 404,
            message: `Document not found: ${collectionId}/${docId}`,
            status: 'NOT_FOUND',
          },
        });
        return;
      }

      res.status(200).json({});
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      res.status(500).json({
        error: {
          code: 500,
          message: errorMessage,
          status: 'INTERNAL_ERROR',
        },
      });
    }
  }

  public async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.serverInstance = this.app.listen(
          this.config.port,
          this.config.host,
          () => {
            // eslint-disable-next-line no-console
            console.log(
              `Firestore emulator server running on http://${this.config.host}:${this.config.port}`,
            );
            // eslint-disable-next-line no-console
            console.log(`Project ID: ${this.config.projectId || 'default'}`);
            resolve();
          },
        );

        this.serverInstance.on('error', (error) => {
          reject(error instanceof Error ? error : new Error(String(error)));
        });
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  public async stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.serverInstance) {
        resolve();
        return;
      }

      this.serverInstance.close((error) => {
        if (error) {
          reject(error);
        } else {
          // eslint-disable-next-line no-console
          console.log('Firestore emulator server stopped');
          resolve();
        }
      });
    });
  }

  public getStorage(): FirestoreStorage {
    return this.storage;
  }

  public getConfig(): Readonly<ServerConfig> {
    return { ...this.config };
  }
}
