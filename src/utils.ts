/**
 * Utility functions for Firestore operations
 */

import { FirestoreValue, FirestoreDocument } from './types.js';

/**
 * Convert a JavaScript value to Firestore value format
 */
export function toFirestoreValue(value: any): FirestoreValue {
  if (value === null || value === undefined) {
    return { nullValue: null };
  }
  if (typeof value === 'boolean') {
    return { booleanValue: value };
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { integerValue: value.toString() };
    }
    return { doubleValue: value };
  }
  if (typeof value === 'string') {
    return { stringValue: value };
  }
  if (value instanceof Date) {
    return {
      timestampValue: value.toISOString(),
    };
  }
  if (Array.isArray(value)) {
    return {
      arrayValue: {
        values: value.map(toFirestoreValue),
      },
    };
  }
  if (typeof value === 'object') {
    return {
      mapValue: {
        fields: Object.keys(value).reduce(
          (acc, key) => {
            acc[key] = toFirestoreValue(value[key]);
            return acc;
          },
          {} as Record<string, FirestoreValue>,
        ),
      },
    };
  }
  return { nullValue: null };
}

/**
 * Convert Firestore value format to JavaScript value
 */
export function fromFirestoreValue(firestoreValue: FirestoreValue): any {
  if ('nullValue' in firestoreValue) {
    return null;
  }
  if ('booleanValue' in firestoreValue) {
    return firestoreValue.booleanValue;
  }
  if ('integerValue' in firestoreValue) {
    return parseInt(firestoreValue.integerValue!, 10);
  }
  if ('doubleValue' in firestoreValue) {
    return firestoreValue.doubleValue;
  }
  if ('stringValue' in firestoreValue) {
    return firestoreValue.stringValue;
  }
  if ('timestampValue' in firestoreValue) {
    return new Date(firestoreValue.timestampValue!);
  }
  if ('arrayValue' in firestoreValue) {
    return firestoreValue.arrayValue!.values.map(fromFirestoreValue);
  }
  if ('mapValue' in firestoreValue) {
    const result: Record<string, any> = {};
    Object.keys(firestoreValue.mapValue!.fields).forEach((key) => {
      result[key] = fromFirestoreValue(firestoreValue.mapValue!.fields[key]);
    });
    return result;
  }
  return null;
}

/**
 * Convert a plain JavaScript object to Firestore document format
 */
export function toFirestoreDocument(
  name: string,
  data: Record<string, any>,
): FirestoreDocument {
  const fields: Record<string, FirestoreValue> = {};
  Object.keys(data).forEach((key) => {
    fields[key] = toFirestoreValue(data[key]);
  });

  const now = new Date().toISOString();
  return {
    name,
    fields,
    createTime: now,
    updateTime: now,
  };
}

/**
 * Convert Firestore document to plain JavaScript object
 */
export function fromFirestoreDocument(
  document: FirestoreDocument,
): Record<string, any> {
  const result: Record<string, any> = {};
  Object.keys(document.fields).forEach((key) => {
    result[key] = fromFirestoreValue(document.fields[key]);
  });
  return result;
}

/**
 * Generate a document ID (simple implementation, can be improved)
 */
export function generateDocumentId(): string {
  // Simple ID generation - in production, use Firebase's auto-id algorithm
  return (
    Math.random().toString(36).substring(2, 15) +
    Math.random().toString(36).substring(2, 15)
  );
}

/**
 * Parse document path
 */
export function parseDocumentPath(path: string): {
  projectId: string;
  databaseId: string;
  collectionId: string;
  docId: string;
} {
  // Format: projects/{projectId}/databases/{databaseId}/documents/{collectionId}/{docId}
  const match =
    /^projects\/([^\/]+)\/databases\/([^\/]+)\/documents\/([^\/]+)\/(.+)$/.exec(
      path,
    );
  if (!match) {
    throw new Error(`Invalid document path: ${path}`);
  }
  return {
    projectId: match[1],
    databaseId: match[2],
    collectionId: match[3],
    docId: match[4],
  };
}

/**
 * Build document path
 */
export function buildDocumentPath(
  projectId: string,
  databaseId: string,
  collectionId: string,
  docId: string,
): string {
  return `projects/${projectId}/databases/${databaseId}/documents/${collectionId}/${docId}`;
}
