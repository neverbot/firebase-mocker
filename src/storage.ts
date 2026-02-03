/**
 * In-memory storage for Firestore documents
 */

import {
  FirestoreDocument,
  FirestoreCollection,
  FirestoreDatabase,
  FirestoreProject,
} from './types';

export class Storage {
  private readonly projects = new Map<string, FirestoreProject>();

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
   * List all documents in a collection
   */
  listDocuments(
    projectId: string,
    databaseId: string,
    collectionId: string,
  ): FirestoreDocument[] {
    const collection = this.getCollection(projectId, databaseId, collectionId);
    return Object.values(collection);
  }

  /**
   * Clear all data (useful for testing)
   */
  clear(): void {
    this.projects.clear();
  }
}
