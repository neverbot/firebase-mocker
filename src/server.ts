/**
 * gRPC server that emulates Firestore API
 */

import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { FirestoreStorage } from './firestore-storage';
import { getLogger } from './logger';
import { ServerConfig } from './types';
import {
  toFirestoreDocument,
  fromFirestoreDocument,
  buildDocumentPath,
  generateDocumentId,
} from './utils';

export class FirestoreServer {
  private readonly storage: FirestoreStorage;
  private readonly config: ServerConfig;
  private grpcServer?: grpc.Server;
  private readonly logger = getLogger();

  constructor(config: ServerConfig) {
    this.config = config;
    this.storage = new FirestoreStorage();
  }

  /**
   * Parse document path like "projects/{project}/databases/{db}/documents/{collection}/{doc}"
   */
  private parseDocumentPath(path: string): {
    projectId: string;
    databaseId: string;
    collectionId: string;
    docId: string;
  } | null {
    const parts = path.split('/');
    const projectIndex = parts.indexOf('projects');
    const dbIndex = parts.indexOf('databases');
    const docsIndex = parts.indexOf('documents');

    if (
      projectIndex === -1 ||
      dbIndex === -1 ||
      docsIndex === -1 ||
      projectIndex + 1 >= parts.length ||
      dbIndex + 1 >= parts.length ||
      docsIndex + 1 >= parts.length
    ) {
      return null;
    }

    const projectId = parts[projectIndex + 1];
    const databaseId = parts[dbIndex + 1];
    const collectionId = parts[docsIndex + 1] || '';
    const docId = parts[docsIndex + 2] || '';

    return { projectId, databaseId, collectionId, docId };
  }

  /**
   * Handle GetDocument gRPC call
   */
  private handleGetDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.name || '';

      this.logger.log('grpc', `GetDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
      if (!parsed) {
        this.logger.log(
          'grpc',
          `GetDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      const document = this.storage.getDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      if (!document) {
        this.logger.log(
          'grpc',
          `GetDocument response: NOT_FOUND - Document not found`,
        );
        callback({
          code: grpc.status.NOT_FOUND,
          message: `Document not found: ${path}`,
        });
        return;
      }

      this.logger.log('grpc', `GetDocument response: SUCCESS - Document found`);
      callback(null, document);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle ListDocuments gRPC call
   */
  private handleListDocuments(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const parent = request.parent || '';
      const collectionId = request.collectionId || '';

      this.logger.log(
        'grpc',
        `ListDocuments request: parent=${parent}, collectionId=${collectionId}`,
      );

      // Parse parent path like "projects/{project}/databases/{db}/documents"
      const parts = parent.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');
      const docsIndex = parts.indexOf('documents');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        docsIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `ListDocuments response: ERROR - Invalid parent path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid parent path: ${parent}`,
        });
        return;
      }

      const projectId = parts[projectIndex + 1];
      const databaseId = parts[dbIndex + 1];

      if (!collectionId) {
        this.logger.log(
          'grpc',
          `ListDocuments response: ERROR - collectionId required`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: 'collectionId is required',
        });
        return;
      }

      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionId,
      );

      this.logger.log(
        'grpc',
        `ListDocuments response: SUCCESS - Found ${documents.length} documents`,
      );
      callback(null, {
        documents,
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle RunQuery gRPC call
   * This is the method used by collection.get() in Firebase Admin SDK
   * RunQuery is a server streaming RPC (client sends request, server streams responses)
   */
  private handleRunQuery(call: grpc.ServerWritableStream<any, any>): void {
    try {
      const request = call.request;
      let parent = request.parent || '';
      const structuredQuery = request.structured_query || {};
      const from = structuredQuery.from;

      // Normalize parent path: replace (default) with default
      parent = parent.replace('/databases/(default)/', '/databases/default/');

      // Parse parent path to extract projectId, databaseId
      const parts = parent.split('/');
      const projectId = parts[1] || 'test-project';
      const databaseId = parts[3] || 'default';

      // Get collection ID from the query
      // The from field can be an array of CollectionSelector objects
      let collectionId = '';
      if (from) {
        if (Array.isArray(from) && from.length > 0) {
          const firstFrom = from[0];
          if (firstFrom && firstFrom.collection_id) {
            collectionId = firstFrom.collection_id;
          }
        } else if (from.collection_id) {
          // Handle case where from is a single object
          collectionId = from.collection_id;
        }
      }

      this.logger.log(
        'grpc',
        `RunQuery request: parent=${parent}, collectionId=${collectionId || '(empty)'}`,
      );

      // Get documents from storage
      const documents = this.storage.listDocuments(
        projectId,
        databaseId,
        collectionId,
      );

      // Convert current time to Timestamp format (seconds and nanos)
      const now = new Date();
      const timestamp = {
        seconds: Math.floor(now.getTime() / 1000),
        nanos: (now.getTime() % 1000) * 1000000,
      };

      // Send responses as a stream
      if (documents.length === 0) {
        // For empty collections, send a response with readTime but no document
        const emptyResponse = {
          read_time: timestamp,
          skipped_results: 0,
        };
        call.write(emptyResponse);
        this.logger.log(
          'grpc',
          `RunQuery response: SUCCESS - Empty collection (0 documents)`,
        );
      } else {
        // Send each document as a stream response
        documents.forEach((doc) => {
          const documentPath = buildDocumentPath(
            projectId,
            databaseId,
            collectionId,
            doc.name.split('/').pop() || '',
          );

          const grpcDocument = {
            document: {
              name: documentPath,
              fields: fromFirestoreDocument(doc),
              create_time: doc.createTime,
              update_time: doc.updateTime,
            },
            read_time: timestamp,
            skipped_results: 0,
          };

          call.write(grpcDocument);
        });
        this.logger.log(
          'grpc',
          `RunQuery response: SUCCESS - Streamed ${documents.length} documents`,
        );
      }

      // End the stream
      call.end();
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      call.destroy({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      } as grpc.ServiceError);
    }
  }

  /**
   * Handle CreateDocument gRPC call
   */
  private handleCreateDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const parent = request.parent || '';
      const collectionId = request.collectionId || '';
      const docId = request.documentId || 'auto-generated';

      this.logger.log(
        'grpc',
        `CreateDocument request: parent=${parent}, collectionId=${collectionId}, documentId=${docId}`,
      );

      // Parse parent path
      const parts = parent.split('/');
      const projectIndex = parts.indexOf('projects');
      const dbIndex = parts.indexOf('databases');

      if (
        projectIndex === -1 ||
        dbIndex === -1 ||
        projectIndex + 1 >= parts.length ||
        dbIndex + 1 >= parts.length
      ) {
        this.logger.log(
          'grpc',
          `CreateDocument response: ERROR - Invalid parent path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid parent path: ${parent}`,
        });
        return;
      }

      const projectId = parts[projectIndex + 1];
      const databaseId = parts[dbIndex + 1];
      const finalDocId = request.documentId || generateDocumentId();

      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        finalDocId,
      );

      // Convert request document to Firestore format
      const fields = request.document?.fields || {};
      const document = toFirestoreDocument(documentPath, fields);
      document.name = documentPath;

      this.storage.setDocument(
        projectId,
        databaseId,
        collectionId,
        finalDocId,
        document,
      );

      this.logger.log(
        'grpc',
        `CreateDocument response: SUCCESS - Created document at ${documentPath}`,
      );
      callback(null, document);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle UpdateDocument gRPC call
   */
  private handleUpdateDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.document?.name || '';

      this.logger.log('grpc', `UpdateDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
      if (!parsed) {
        this.logger.log(
          'grpc',
          `UpdateDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      // Get existing document or create new one
      const existingDoc = this.storage.getDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      const fields = request.document?.fields || {};
      const document = toFirestoreDocument(path, fields);
      document.name = path;

      if (existingDoc) {
        document.createTime = existingDoc.createTime;
      }
      document.updateTime = new Date().toISOString();

      this.storage.setDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
        document,
      );

      this.logger.log(
        'grpc',
        `UpdateDocument response: SUCCESS - ${existingDoc ? 'Updated' : 'Created'} document at ${path}`,
      );
      callback(null, document);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  /**
   * Handle DeleteDocument gRPC call
   */
  private handleDeleteDocument(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>,
  ): void {
    try {
      const request = call.request;
      const path = request.name || '';

      this.logger.log('grpc', `DeleteDocument request: path=${path}`);

      const parsed = this.parseDocumentPath(path);
      if (!parsed) {
        this.logger.log(
          'grpc',
          `DeleteDocument response: ERROR - Invalid document path`,
        );
        callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid document path: ${path}`,
        });
        return;
      }

      const deleted = this.storage.deleteDocument(
        parsed.projectId,
        parsed.databaseId,
        parsed.collectionId,
        parsed.docId,
      );

      if (!deleted) {
        this.logger.log(
          'grpc',
          `DeleteDocument response: NOT_FOUND - Document not found`,
        );
        callback({
          code: grpc.status.NOT_FOUND,
          message: `Document not found: ${path}`,
        });
        return;
      }

      this.logger.log(
        'grpc',
        `DeleteDocument response: SUCCESS - Document deleted`,
      );
      callback(null, {});
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      callback({
        code: grpc.status.INTERNAL,
        message: errorMessage,
      });
    }
  }

  public async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.grpcServer = new grpc.Server();

        // Create service implementation
        const serviceImplementation: any = {
          GetDocument: this.handleGetDocument.bind(this),
          ListDocuments: this.handleListDocuments.bind(this),
          RunQuery: this.handleRunQuery.bind(this),
          CreateDocument: this.handleCreateDocument.bind(this),
          UpdateDocument: this.handleUpdateDocument.bind(this),
          DeleteDocument: this.handleDeleteDocument.bind(this),
        };

        // Load proto file
        const protoPath = path.join(__dirname, '../proto/firestore.proto');

        const packageDefinition = protoLoader.loadSync(protoPath, {
          keepCase: true,
          longs: String,
          enums: String,
          defaults: true,
          oneofs: true,
        });

        const firestoreProto = grpc.loadPackageDefinition(
          packageDefinition,
        ) as any;

        // Get the Firestore service
        const firestoreService =
          firestoreProto.google?.firestore?.v1?.Firestore;

        if (!firestoreService) {
          reject(
            new Error('Failed to load Firestore service from proto definition'),
          );
          return;
        }

        // Add service to server using the loaded proto definition
        this.grpcServer.addService(
          firestoreService.service,
          serviceImplementation,
        );

        // Bind server to address
        // Use IPv6 [::] to support both IPv4 and IPv6 connections
        const bindHost =
          this.config.host === 'localhost' ? '[::]' : this.config.host;
        const isIPv6 = bindHost === '[::]';
        const address = `${bindHost}:${this.config.port}`;
        this.grpcServer.bindAsync(
          address,
          grpc.ServerCredentials.createInsecure(),
          (error, port) => {
            if (error) {
              reject(error);
              return;
            }

            // DeprecationWarning: Calling start() is no longer necessary. It can be safely omitted.
            // this.grpcServer!.start();

            this.logger.log(
              'server',
              `Firestore gRPC emulator server running on ${this.config.host}:${port}${isIPv6 ? ' (IPv6 [::], accepts both IPv4 and IPv6 connections)' : ''}`,
            );
            this.logger.log(
              'server',
              `Project ID: ${this.config.projectId || 'default'}`,
            );

            resolve();
          },
        );
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  public async stop(): Promise<void> {
    return new Promise((resolve) => {
      const promises: Promise<void>[] = [];

      if (this.grpcServer) {
        this.logger.log('grpc', 'Stopping server...');
        this.grpcServer.forceShutdown();
        this.grpcServer = undefined;
        this.logger.log('server', 'Firestore gRPC emulator server stopped');
      }

      Promise.all(promises).then(() => resolve());
      if (promises.length === 0) {
        resolve();
      }
    });
  }

  public getStorage(): FirestoreStorage {
    return this.storage;
  }

  public getConfig(): Readonly<ServerConfig> {
    return { ...this.config };
  }
}
