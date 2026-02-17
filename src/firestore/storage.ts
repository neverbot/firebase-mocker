/**
 * In-memory storage for Firestore documents
 */

import { getLogger } from '../logger';
import {
  FirestoreDocument,
  FirestoreCollection,
  FirestoreDatabase,
  FirestoreProject,
  FirestoreValue,
} from '../types';

export class FirestoreStorage {
  private readonly projects = new Map<string, FirestoreProject>();
  private readonly logger = getLogger();

  /**
   * Get or create a project
   */
  private getProject(projectId: string): FirestoreProject {
    if (!this.projects.has(projectId)) {
      this.projects.set(projectId, {});
    }
    return this.projects.get(projectId)!;
  }

  /**
   * Get or create a database (default is '(default)')
   */
  private getDatabase(
    projectId: string,
    databaseId = '(default)',
  ): FirestoreDatabase {
    const project = this.getProject(projectId);
    if (!project[databaseId]) {
      project[databaseId] = {};
    }
    return project[databaseId];
  }

  /**
   * Get or create a collection
   */
  private getCollection(
    projectId: string,
    databaseId: string,
    collectionId: string,
  ): FirestoreCollection {
    const database = this.getDatabase(projectId, databaseId);
    if (!database[collectionId]) {
      database[collectionId] = {};
    }
    return database[collectionId];
  }

  /**
   * Get a document by path
   */
  getDocument(
    projectId: string,
    databaseId: string,
    collectionId: string,
    docId: string,
  ): FirestoreDocument | null {
    const collection = this.getCollection(projectId, databaseId, collectionId);
    return collection[docId] || null;
  }

  /**
   * Set a document
   */
  setDocument(
    projectId: string,
    databaseId: string,
    collectionId: string,
    docId: string,
    document: FirestoreDocument,
  ): void {
    const collection = this.getCollection(projectId, databaseId, collectionId);
    collection[docId] = document;
  }

  /**
   * Delete a document
   */
  deleteDocument(
    projectId: string,
    databaseId: string,
    collectionId: string,
    docId: string,
  ): boolean {
    const collection = this.getCollection(projectId, databaseId, collectionId);
    if (collection[docId]) {
      delete collection[docId];
      return true;
    }
    return false;
  }

  /**
   * List all documents in a collection.
   * Returns deep-cloned documents so consumers never touch storage-backed objects.
   */
  listDocuments(
    projectId: string,
    databaseId: string,
    collectionId: string,
  ): FirestoreDocument[] {
    const collection = this.getCollection(projectId, databaseId, collectionId);
    const docs = Object.values(collection);
    return docs.map((doc) => {
      try {
        const seen = new WeakSet<object>();
        const json = JSON.stringify(doc, (_, v) => {
          if (v !== null && typeof v === 'object') {
            if (seen.has(v)) {
              return undefined;
            }
            seen.add(v);
          }
          return v;
        });
        return JSON.parse(json) as FirestoreDocument;
      } catch {
        return {
          name: doc.name,
          fields: {},
          createTime: doc.createTime ?? '',
          updateTime: doc.updateTime ?? '',
        } as FirestoreDocument;
      }
    });
  }

  /**
   * List collection IDs under a parent path.
   * @param projectId Project ID
   * @param databaseId Database ID (e.g. '(default)')
   * @param parentPath Path after /documents/ (e.g. '' for root, 'events/eventId' for subcollections)
   * @returns Array of collection ID strings (e.g. ['users', 'agenda'] for subcollections)
   */
  listCollectionIds(
    projectId: string,
    databaseId: string,
    parentPath: string,
  ): string[] {
    const database = this.getDatabase(projectId, databaseId);
    const keys = Object.keys(database);
    const prefix = parentPath ? `${parentPath}/` : '';
    const collectionIds = new Set<string>();
    for (const key of keys) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      const suffix = key.slice(prefix.length);
      const nextSlash = suffix.indexOf('/');
      if (nextSlash === -1) {
        if (suffix.length > 0) {
          collectionIds.add(suffix);
        }
      }
    }
    return Array.from(collectionIds).sort();
  }

  /**
   * Clear all data (useful for testing)
   */
  clear(): void {
    this.projects.clear();
  }

  /**
   * Log all content in storage for debugging purposes
   * Prints a formatted representation of all projects, databases, collections, and documents
   */
  debugLog(): void {
    if (this.projects.size === 0) {
      this.logger.logMessage('Storage is empty');
      return;
    }

    this.logger.logMessage('=== Storage Debug Log ===');
    this.logger.logMessage(`Total projects: ${this.projects.size}`);

    for (const [projectId, project] of this.projects.entries()) {
      const databases = Object.keys(project);
      this.logger.logMessage(
        `\nProject: ${projectId} (databases: ${databases.length})`,
      );

      for (const [databaseId, database] of Object.entries(project)) {
        const collections = Object.keys(database);
        this.logger.logMessage(
          `  Database: ${databaseId} (collections: ${collections.length})`,
        );

        for (const [collectionId, collection] of Object.entries(database)) {
          const documents = Object.keys(collection);
          this.logger.logMessage(
            `    Collection: ${collectionId} (documents: ${documents.length})`,
          );

          for (const [docId, document] of Object.entries(collection)) {
            this.logger.logMessage(`      Document: ${docId}`);
            this.logger.logMessage(`        Path: ${document.name}`);
            if (document.createTime) {
              this.logger.logMessage(
                `        Create Time: ${document.createTime}`,
              );
            }
            if (document.updateTime) {
              this.logger.logMessage(
                `        Update Time: ${document.updateTime}`,
              );
            }

            const fields = Object.keys(document.fields);
            if (fields.length > 0) {
              this.logger.logMessage(`        Fields (${fields.length}):`);
              for (const [fieldName, fieldValue] of Object.entries(
                document.fields,
              )) {
                const fieldType = document.fieldTypes?.[fieldName] || 'unknown';
                const valueStr = this.formatFieldValue(fieldValue);
                this.logger.logMessage(
                  `          - ${fieldName} (${fieldType}): ${valueStr}`,
                );
              }
            } else {
              this.logger.logMessage('        Fields: (empty)');
            }
          }
        }
      }
    }

    this.logger.logMessage('\n=== End Storage Debug Log ===');
  }

  /**
   * Format a FirestoreValue for display in logs
   */
  private formatFieldValue(value: FirestoreValue, indent = 0): string {
    const indentStr = '  '.repeat(indent);

    if (value.nullValue !== undefined) {
      return 'null';
    }
    if (value.booleanValue !== undefined) {
      return String(value.booleanValue);
    }
    if (value.integerValue !== undefined) {
      return value.integerValue;
    }
    if (value.doubleValue !== undefined) {
      return String(value.doubleValue);
    }
    if (value.stringValue !== undefined) {
      return `"${value.stringValue}"`;
    }
    if (value.timestampValue !== undefined) {
      const timestamp = value.timestampValue;
      // Handle both string (ISO) and object (seconds/nanos) formats
      if (typeof timestamp === 'string') {
        return `Timestamp(${timestamp})`;
      }
      if (typeof timestamp === 'object' && timestamp !== null) {
        // Handle gRPC Timestamp format with seconds and nanos
        const seconds = (timestamp as any).seconds;
        const nanos = (timestamp as any).nanos;
        if (seconds !== undefined) {
          // Convert seconds (and nanos) to Date
          const ms =
            typeof seconds === 'number'
              ? seconds * 1000 + (nanos ? nanos / 1000000 : 0)
              : parseInt(String(seconds), 10) * 1000 +
                (nanos ? nanos / 1000000 : 0);
          const date = new Date(ms);
          return `Timestamp(${date.toISOString()})`;
        }
        // Fallback: try to convert to string
        try {
          return `Timestamp(${JSON.stringify(timestamp)})`;
        } catch {
          return `Timestamp(${String(timestamp)})`;
        }
      }
      return `Timestamp(${String(timestamp)})`;
    }
    if (value.bytesValue !== undefined) {
      return `Bytes(${value.bytesValue.length} bytes)`;
    }
    if (value.referenceValue !== undefined) {
      return `Reference(${value.referenceValue})`;
    }
    if (value.geoPointValue !== undefined) {
      return `GeoPoint(${value.geoPointValue.latitude}, ${value.geoPointValue.longitude})`;
    }
    if (value.arrayValue !== undefined) {
      if (value.arrayValue.values.length === 0) {
        return '[]';
      }
      const items = value.arrayValue.values
        .map((v) => this.formatFieldValue(v, indent + 1))
        .join(', ');
      return `[${items}]`;
    }
    if (value.mapValue !== undefined) {
      const fields = Object.keys(value.mapValue.fields);
      if (fields.length === 0) {
        return '{}';
      }
      const items = fields
        .map(
          (key) =>
            `${indentStr}  ${key}: ${this.formatFieldValue(value.mapValue!.fields[key], indent + 1)}`,
        )
        .join('\n');
      return `{\n${items}\n${indentStr}}`;
    }

    return '(unknown)';
  }
}
