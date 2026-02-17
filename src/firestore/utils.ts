/**
 * Firestore emulator utilities: atomic, independent functions for paths,
 * document reconstruction, query filters, ordering, and value comparison.
 */

import * as grpc from '@grpc/grpc-js';
import { config } from '../config';
import {
  FirestoreDocument,
  FirestoreValue,
  FieldType,
  FIELD_TYPE_KEYS,
} from '../types';

/**
 * Firestore logger type
 * @param category - The category of the log
 * @param message - The message to log
 * @returns The log
 * @example
 * log('firestore', 'my-message') // logs 'my-message'
 */
export type FirestoreLogger =
  | {
      log(category: string, message: string): void;
    }
  | undefined;

/** gRPC NullValue = 0; protobufjs skips null when encoding. */
export const GRPC_NULL_VALUE = 0;

/**
 * Convert a date to a timestamp
 * @param date - The date to convert
 * @returns The timestamp
 * @example
 * toTimestamp(new Date()) // returns { seconds: 1609459200, nanos: 0 }
 */
export function toTimestamp(date: Date): { seconds: number; nanos: number } {
  const ms = date.getTime();
  return {
    seconds: Math.floor(ms / 1000),
    nanos: (ms % 1000) * 1000000,
  };
}

/**
 * Convert a value to a Firestore value
 * @param value - The value to convert
 * @returns The Firestore value
 * @example
 * toFirestoreValue('hello') // returns { stringValue: 'hello' }
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
    return { timestampValue: value.toISOString() };
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
 * Convert a Firestore value to a value
 * @param firestoreValue - The Firestore value to convert
 * @returns The value
 * @example
 * fromFirestoreValue({ stringValue: 'hello' }) // returns 'hello'
 */
export function fromFirestoreValue(firestoreValue: FirestoreValue): any {
  if (!firestoreValue || typeof firestoreValue !== 'object') {
    return null;
  }
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
 * Convert a document to a Firestore document
 * @param name - The name of the document
 * @param data - The data of the document
 * @returns The Firestore document
 * @example
 * toFirestoreDocument('projects/my-project/databases/my-database/documents/my-collection/my-document', { name: 'John Doe' }) // returns { name: 'John Doe' }
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
 * Convert a Firestore document to a document
 * @param document - The Firestore document to convert
 * @returns The document
 * @example
 * fromFirestoreDocument({ name: 'projects/my-project/databases/my-database/documents/my-collection/my-document', fields: { name: { stringValue: 'John Doe' } } }) // returns { name: 'John Doe' }
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
 * Generate a document ID
 * @returns The document ID
 * @example
 * generateDocumentId() // returns '1234567890'
 */
export function generateDocumentId(): string {
  return (
    Math.random().toString(36).substring(2, 15) +
    Math.random().toString(36).substring(2, 15)
  );
}

/**
 * Maximum normalize depth
 */
const MAX_NORMALIZE_DEPTH = 50;

/**
 * Normalize a gRPC value to a Firestore value
 * @param value - The gRPC value to normalize
 * @param options - The options to use for normalization
 * @returns The normalized Firestore value
 * @example
 * normalizeGrpcValueToFirestoreValue({ stringValue: 'hello' }) // returns { stringValue: 'hello' }
 */
export function normalizeGrpcValueToFirestoreValue(
  value: any,
  options?: { visited?: WeakSet<object>; depth?: number },
): FirestoreValue {
  const visited = options?.visited ?? new WeakSet<object>();
  const depth = options?.depth ?? 0;

  if (depth > MAX_NORMALIZE_DEPTH) {
    return { nullValue: null };
  }
  if (!value || typeof value !== 'object') {
    return { nullValue: null };
  }
  if (visited.has(value)) {
    return { nullValue: null };
  }
  visited.add(value);

  const nextOpts = { visited, depth: depth + 1 };

  if ('null_value' in value || 'nullValue' in value) {
    return { nullValue: null };
  }
  if ('boolean_value' in value || 'booleanValue' in value) {
    return { booleanValue: value.boolean_value || value.booleanValue };
  }
  if ('integer_value' in value || 'integerValue' in value) {
    return { integerValue: value.integer_value || value.integerValue };
  }
  if ('double_value' in value || 'doubleValue' in value) {
    return { doubleValue: value.double_value || value.doubleValue };
  }
  if ('string_value' in value || 'stringValue' in value) {
    return { stringValue: value.string_value || value.stringValue };
  }

  const isTimestamp =
    'timestamp_value' in value ||
    'timestampValue' in value ||
    value?.valueType === 'timestampValue' ||
    value?.value_type === 'timestampValue';
  if (isTimestamp) {
    const raw = value.timestamp_value ?? value.timestampValue;
    let iso: string;
    if (typeof raw === 'string') {
      iso = raw;
    } else if (raw && typeof raw === 'object' && 'seconds' in raw) {
      const sec = Number(raw.seconds) || 0;
      const nan = Number(raw.nanos) || 0;
      iso = new Date(sec * 1000 + nan / 1000000).toISOString();
    } else {
      iso = new Date().toISOString();
    }
    return { timestampValue: iso };
  }

  if ('bytes_value' in value || 'bytesValue' in value) {
    return { bytesValue: value.bytes_value || value.bytesValue };
  }
  if ('reference_value' in value || 'referenceValue' in value) {
    return { referenceValue: value.reference_value || value.referenceValue };
  }
  if ('geo_point_value' in value || 'geoPointValue' in value) {
    return { geoPointValue: value.geo_point_value || value.geoPointValue };
  }
  if ('array_value' in value || 'arrayValue' in value) {
    const arrayVal = value.array_value || value.arrayValue;
    if (arrayVal && arrayVal.values) {
      return {
        arrayValue: {
          values: arrayVal.values.map((v: any) =>
            normalizeGrpcValueToFirestoreValue(v, nextOpts),
          ),
        },
      };
    }
    return { arrayValue: { values: [] } };
  }
  if ('map_value' in value || 'mapValue' in value) {
    const mapVal = value.map_value || value.mapValue;
    if (mapVal && mapVal.fields) {
      const normalizedFields: Record<string, FirestoreValue> = {};
      Object.keys(mapVal.fields).forEach((key) => {
        normalizedFields[key] = normalizeGrpcValueToFirestoreValue(
          mapVal.fields[key],
          nextOpts,
        );
      });
      return { mapValue: { fields: normalizedFields } };
    }
    return { mapValue: { fields: {} } };
  }
  return { nullValue: null };
}

/**
 * Convert a Firestore value to a gRPC value
 * @param firestoreValue - The Firestore value to convert
 * @returns The gRPC value
 * @example
 * toGrpcValue({ stringValue: 'hello' }) // returns { stringValue: 'hello' }
 */
export function toGrpcValue(firestoreValue: FirestoreValue): any {
  if (!firestoreValue || typeof firestoreValue !== 'object') {
    return { nullValue: GRPC_NULL_VALUE };
  }
  const hasExactlyOne =
    FIELD_TYPE_KEYS.filter((k) => k in firestoreValue).length === 1;
  if (!hasExactlyOne) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if ('nullValue' in firestoreValue) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if ('booleanValue' in firestoreValue) {
    return { booleanValue: firestoreValue.booleanValue };
  }
  if ('integerValue' in firestoreValue) {
    return { integerValue: firestoreValue.integerValue };
  }
  if ('doubleValue' in firestoreValue) {
    return { doubleValue: firestoreValue.doubleValue };
  }
  if ('stringValue' in firestoreValue) {
    return { stringValue: firestoreValue.stringValue };
  }
  if ('timestampValue' in firestoreValue) {
    const tv = firestoreValue.timestampValue;
    if (typeof tv === 'string') {
      return { timestampValue: toTimestamp(new Date(tv)) };
    }
    if (tv && typeof tv === 'object' && 'seconds' in tv) {
      return { timestampValue: tv };
    }
    return { timestampValue: toTimestamp(new Date()) };
  }
  if ('bytesValue' in firestoreValue) {
    return { bytesValue: firestoreValue.bytesValue };
  }
  if ('referenceValue' in firestoreValue) {
    return { referenceValue: firestoreValue.referenceValue };
  }
  if ('geoPointValue' in firestoreValue) {
    return { geoPointValue: firestoreValue.geoPointValue };
  }
  if ('arrayValue' in firestoreValue) {
    if (firestoreValue.arrayValue && firestoreValue.arrayValue.values) {
      return {
        arrayValue: {
          values: firestoreValue.arrayValue.values.map(toGrpcValue),
        },
      };
    }
    return { arrayValue: { values: [] } };
  }
  if ('mapValue' in firestoreValue) {
    if (firestoreValue.mapValue && firestoreValue.mapValue.fields) {
      return {
        mapValue: {
          fields: toGrpcFields(firestoreValue.mapValue.fields),
        },
      };
    }
    return { mapValue: { fields: {} } };
  }
  return { nullValue: GRPC_NULL_VALUE };
}

/**
 * Sanitize a gRPC value for response
 * @param value - The gRPC value to sanitize
 * @returns The sanitized gRPC value
 * @example
 * sanitizeGrpcValueForResponse({ stringValue: 'hello' }) // returns { stringValue: 'hello' }
 */
export function sanitizeGrpcValueForResponse(value: any): any {
  if (value === null || value === undefined) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if (typeof value !== 'object') {
    return value;
  }
  const valueKeyCount = FIELD_TYPE_KEYS.filter((k) => k in value).length;
  if (valueKeyCount === 0) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if (valueKeyCount > 1) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if (value.arrayValue && value.arrayValue.values) {
    return {
      ...value,
      arrayValue: {
        values: value.arrayValue.values.map(sanitizeGrpcValueForResponse),
      },
    };
  }
  if (
    value.mapValue &&
    value.mapValue.fields &&
    typeof value.mapValue.fields === 'object'
  ) {
    const fields: Record<string, any> = {};
    Object.keys(value.mapValue.fields).forEach((k) => {
      fields[k] = sanitizeGrpcValueForResponse(value.mapValue.fields[k]);
    });
    return { mapValue: { fields } };
  }
  return value;
}

/**
 * Sanitize gRPC fields for response
 * @param fields - The gRPC fields to sanitize
 * @returns The sanitized gRPC fields
 * @example
 * sanitizeGrpcFieldsForResponse({ name: { stringValue: 'John Doe' } }) // returns { name: { stringValue: 'John Doe' } }
 */
export function sanitizeGrpcFieldsForResponse(
  fields: Record<string, any>,
): Record<string, any> {
  const out: Record<string, any> = {};
  Object.keys(fields).forEach((key) => {
    out[key] = sanitizeGrpcValueForResponse(fields[key]);
  });
  return out;
}

/**
 * Convert Firestore fields to gRPC fields
 * @param fields - The Firestore fields to convert
 * @returns The gRPC fields
 * @example
 * toGrpcFields({ name: { stringValue: 'John Doe' } }) // returns { name: { stringValue: 'John Doe' } }
 */
export function toGrpcFields(
  fields: Record<string, FirestoreValue>,
): Record<string, any> {
  const grpcFields: Record<string, any> = {};
  Object.keys(fields).forEach((key) => {
    const grpcValue = toGrpcValue(fields[key]);
    if (Object.keys(grpcValue).length > 0) {
      grpcFields[key] = grpcValue;
    }
  });
  return grpcFields;
}

/**
 * Build a document path
 * @param projectId - The project ID
 * @param databaseId - The database ID
 * @param collectionId - The collection ID
 * @param docId - The document ID
 * @returns The document path
 * @example
 * buildDocumentPath('my-project', 'my-database', 'my-collection', 'my-document') // returns 'projects/my-project/databases/my-database/documents/my-collection/my-document'
 */
export function buildDocumentPath(
  projectId: string,
  databaseId: string,
  collectionId: string,
  docId: string,
): string {
  return `projects/${projectId}/databases/${databaseId}/documents/${collectionId}/${docId}`;
}

/**
 * Parse document path like "projects/{project}/databases/{db}/documents/{path...}/{doc}"
 * @param path - The document path to parse
 * @returns The parsed document path
 * @example
 * parseDocumentPath('projects/my-project/databases/my-database/documents/my-collection/my-document')
 * // returns { projectId: 'my-project', databaseId: 'my-database', collectionId: 'my-collection', docId: 'my-document' }
 */
export function parseDocumentPath(path: string): {
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
  const pathSegments = parts.slice(docsIndex + 1);
  if (pathSegments.length < 2) {
    return null;
  }
  const docId = pathSegments[pathSegments.length - 1];
  const collectionId = pathSegments.slice(0, -1).join('/');

  return { projectId, databaseId, collectionId, docId };
}

/**
 * Infer field type from field name (heuristic for when proto-loader loses oneof data)
 * @param fieldName - The field name to infer the type from
 * @returns The inferred field type
 * @example
 * inferFieldType('items') // returns 'arrayValue'
 * inferFieldType('data') // returns 'mapValue'
 * inferFieldType('name') // returns 'stringValue'
 */
export function inferFieldType(fieldName: string): FieldType {
  const lowerName = fieldName.toLowerCase();
  if (
    lowerName.includes('items') ||
    lowerName.includes('tags') ||
    lowerName.includes('list') ||
    lowerName.includes('array') ||
    lowerName.endsWith('s')
  ) {
    return 'arrayValue';
  }
  if (
    lowerName.includes('data') ||
    lowerName.includes('metadata') ||
    lowerName.includes('config') ||
    lowerName.includes('settings') ||
    lowerName.includes('options')
  ) {
    return 'mapValue';
  }
  return 'arrayValue';
}

/**
 * Detect field type from normalized FirestoreValue
 * @param value - The Firestore value to detect the type from
 * @returns The detected field type
 * @example
 * detectFieldType({ stringValue: 'hello' }) // returns 'stringValue'
 * detectFieldType({ integerValue: '123' }) // returns 'integerValue'
 * detectFieldType({ doubleValue: '123.45' }) // returns 'doubleValue'
 * detectFieldType({ timestampValue: '2021-01-01T00:00:00.000Z' }) // returns 'timestampValue'
 */
export function detectFieldType(value: FirestoreValue): FieldType | null {
  if ('nullValue' in value) {
    return 'nullValue';
  }
  if ('booleanValue' in value) {
    return 'booleanValue';
  }
  if ('integerValue' in value) {
    return 'integerValue';
  }
  if ('doubleValue' in value) {
    return 'doubleValue';
  }
  if ('timestampValue' in value) {
    return 'timestampValue';
  }
  if ('stringValue' in value) {
    return 'stringValue';
  }
  if ('bytesValue' in value) {
    return 'bytesValue';
  }
  if ('referenceValue' in value) {
    return 'referenceValue';
  }
  if ('geoPointValue' in value) {
    return 'geoPointValue';
  }
  if ('arrayValue' in value) {
    return 'arrayValue';
  }
  if ('mapValue' in value) {
    return 'mapValue';
  }
  return null;
}

/**
 * Reconstruct document fields using stored metadata
 * @param document - The document to reconstruct the fields from
 * @param logger - The logger to use for logging
 * @returns The reconstructed document fields
 * @example
 * reconstructDocumentFields({ fields: { name: { stringValue: 'John Doe' } } }) // returns { name: { stringValue: 'John Doe' } }
 * reconstructDocumentFields({ fields: { age: { integerValue: '30' } } }) // returns { age: { integerValue: '30' } }
 * reconstructDocumentFields({ fields: { createdAt: { timestampValue: '2021-01-01T00:00:00.000Z' } } }) // returns { createdAt: { timestampValue: '2021-01-01T00:00:00.000Z' } }
 */
export function reconstructDocumentFields(
  document: FirestoreDocument,
  logger?: FirestoreLogger,
): Record<string, FirestoreValue> {
  if (!document.fieldTypes) {
    return document.fields ?? {};
  }

  const reconstructed: Record<string, FirestoreValue> = {
    ...(document.fields ?? {}),
  };

  Object.keys(document.fieldTypes).forEach((key) => {
    const expectedType = document.fieldTypes![key];
    const currentValue = reconstructed[key];

    if (!currentValue || !(expectedType in currentValue)) {
      logger?.log(
        'grpc',
        `Reconstructing field '${key}' with type '${expectedType}'`,
      );
      switch (expectedType) {
        case 'arrayValue':
          reconstructed[key] = { arrayValue: { values: [] } };
          break;
        case 'mapValue':
          reconstructed[key] = { mapValue: { fields: {} } };
          break;
        case 'nullValue':
          reconstructed[key] = { nullValue: null };
          break;
        case 'booleanValue':
          reconstructed[key] = { booleanValue: false };
          break;
        case 'integerValue':
          reconstructed[key] = { integerValue: '0' };
          break;
        case 'doubleValue':
          reconstructed[key] = { doubleValue: 0 };
          break;
        case 'stringValue':
          reconstructed[key] = { stringValue: '' };
          break;
        default:
          reconstructed[key] = currentValue || { nullValue: null };
      }
    }
  });

  return reconstructed;
}

/**
 * Convert timestampValue (string ISO or object { seconds, nanos }) to milliseconds
 * @param v - The timestamp value to convert to milliseconds
 * @returns The timestamp value in milliseconds
 */
export function timestampToMs(v: FirestoreValue): number | null {
  const raw = (v as any).timestampValue ?? (v as any).timestamp_value;
  if (raw === undefined) {
    return null;
  }
  if (typeof raw === 'string') {
    return new Date(raw).getTime();
  }
  if (raw && typeof raw === 'object' && 'seconds' in raw) {
    const sec = Number(raw.seconds) || 0;
    const nan = Number(raw.nanos) || 0;
    return sec * 1000 + nan / 1000000;
  }
  return null;
}

/**
 * Get field value by path (supports nested fields like "address.city")
 * @param fields - The fields to get the value from
 * @param path - The path to get the value from
 * @returns The value at the path
 * @example
 * getFieldValueByPath({ address: { city: { stringValue: 'New York' } } }, 'address.city') // returns { stringValue: 'New York' }
 */
export function getFieldValueByPath(
  fields: Record<string, FirestoreValue>,
  path: string,
): FirestoreValue | undefined {
  if (!path) {
    return undefined;
  }
  const parts = path.split('.');
  let current: any = fields;

  for (const part of parts) {
    if (!current || typeof current !== 'object') {
      return undefined;
    }
    if (current[part]) {
      current = current[part];
    } else if (current.mapValue && current.mapValue.fields) {
      current = current.mapValue.fields[part];
    } else {
      return undefined;
    }
  }
  return current as FirestoreValue;
}

/**
 * Compare two values for orderBy (returns -1, 0, or 1)
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns -1 if a is less than b, 0 if a is equal to b, 1 if a is greater than b
 * @example
 * compareValuesForOrder({ stringValue: 'apple' }, { stringValue: 'banana' }) // returns -1
 * compareValuesForOrder({ integerValue: '10' }, { integerValue: '20' }) // returns -1
 * compareValuesForOrder({ timestampValue: '2021-01-01T00:00:00.000Z' }, { timestampValue: '2021-01-02T00:00:00.000Z' }) // returns -1
 */
export function compareValuesForOrder(a: any, b: any): number {
  if ((a === null || a === undefined) && (b === null || b === undefined)) {
    return 0;
  }
  if (a === null || a === undefined) {
    return 1;
  }
  if (b === null || b === undefined) {
    return -1;
  }
  const aStr =
    (a as FirestoreValue).stringValue ??
    (typeof a === 'string' ? a : undefined);
  const bStr =
    (b as FirestoreValue).stringValue ??
    (typeof b === 'string' ? b : undefined);
  if (aStr !== undefined && bStr !== undefined) {
    return aStr.localeCompare(bStr);
  }
  const aNum =
    (a as FirestoreValue).integerValue ??
    (a as FirestoreValue).doubleValue ??
    (typeof a === 'number' ? a : undefined);
  const bNum =
    (b as FirestoreValue).integerValue ??
    (b as FirestoreValue).doubleValue ??
    (typeof b === 'number' ? b : undefined);
  if (aNum !== undefined && bNum !== undefined) {
    return Number(aNum) - Number(bNum);
  }
  const aTs = timestampToMs(a as FirestoreValue);
  const bTs = timestampToMs(b as FirestoreValue);
  if (aTs !== null && aTs !== undefined && bTs !== null && bTs !== undefined) {
    return aTs - bTs;
  }
  return String(a).localeCompare(String(b));
}

/**
 * Normalize filter value without invoking proto getters (which can block)
 * @param value - The value to normalize
 * @returns The normalized value
 * @example
 * safeNormalizeFilterValue({ stringValue: 'hello' }) // returns { stringValue: 'hello' }
 * safeNormalizeFilterValue({ integerValue: '123' }) // returns { integerValue: '123' }
 * safeNormalizeFilterValue({ doubleValue: '123.45' }) // returns { doubleValue: '123.45' }
 * safeNormalizeFilterValue({ timestampValue: '2021-01-01T00:00:00.000Z' }) // returns { timestampValue: '2021-01-01T00:00:00.000Z' }
 */
export function safeNormalizeFilterValue(value: any): FirestoreValue {
  if (value === null || value === undefined || typeof value !== 'object') {
    return { nullValue: null };
  }
  let keys: string[];
  try {
    keys = Object.getOwnPropertyNames(value);
  } catch {
    return { nullValue: null };
  }
  const plain: Record<string, unknown> = {};
  for (const key of keys) {
    try {
      const d = Object.getOwnPropertyDescriptor(value, key);
      if (d && 'value' in d) {
        plain[key] = d.value;
      }
    } catch {
      // skip
    }
  }
  return normalizeGrpcValueToFirestoreValue(plain);
}

/**
 * Compare two values for equality
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns True if the values are equal, false otherwise
 * @example
 * valuesEqual({ stringValue: 'hello' }, { stringValue: 'hello' }) // returns true
 * valuesEqual({ integerValue: '123' }, { integerValue: '20' }) // returns false
 * valuesEqual({ timestampValue: '2021-01-01T00:00:00.000Z' }, { timestampValue: '2021-01-02T00:00:00.000Z' }) // returns false
 */
export function valuesEqual(a: FirestoreValue, b: FirestoreValue): boolean {
  if (a.referenceValue !== undefined && b.referenceValue !== undefined) {
    return a.referenceValue === b.referenceValue;
  }
  if (a.stringValue !== undefined && b.stringValue !== undefined) {
    return a.stringValue === b.stringValue;
  }
  if (a.integerValue !== undefined && b.integerValue !== undefined) {
    return String(a.integerValue) === String(b.integerValue);
  }
  if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
    return Number(a.doubleValue) === Number(b.doubleValue);
  }
  if (a.booleanValue !== undefined && b.booleanValue !== undefined) {
    return a.booleanValue === b.booleanValue;
  }
  if (a.nullValue !== undefined && b.nullValue !== undefined) {
    return true;
  }
  const aTs = timestampToMs(a);
  const bTs = timestampToMs(b);
  if (aTs !== null && bTs !== null) {
    return aTs === bTs;
  }
  if (a.timestampValue !== undefined && b.timestampValue !== undefined) {
    return a.timestampValue === b.timestampValue;
  }
  return false;
}

/**
 * Compare two values for less than
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns True if a is less than b, false otherwise
 * @example
 * valueLessThan({ stringValue: 'apple' }, { stringValue: 'banana' }) // returns true
 * valueLessThan({ integerValue: '10' }, { integerValue: '20' }) // returns true
 * valueLessThan({ timestampValue: '2021-01-01T00:00:00.000Z' }, { timestampValue: '2021-01-02T00:00:00.000Z' }) // returns true
 */
export function valueLessThan(a: FirestoreValue, b: FirestoreValue): boolean {
  if (a.stringValue !== undefined && b.stringValue !== undefined) {
    return a.stringValue < b.stringValue;
  }
  if (a.integerValue !== undefined && b.integerValue !== undefined) {
    return Number(a.integerValue) < Number(b.integerValue);
  }
  if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
    return Number(a.doubleValue) < Number(b.doubleValue);
  }
  const aTs = timestampToMs(a);
  const bTs = timestampToMs(b);
  if (aTs !== null && bTs !== null) {
    return aTs < bTs;
  }
  return false;
}

/**
 * Compare two values for greater than
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns True if a is greater than b, false otherwise
 * @example
 * valueGreaterThan({ stringValue: 'apple' }, { stringValue: 'banana' }) // returns false
 * valueGreaterThan({ integerValue: '10' }, { integerValue: '20' }) // returns false
 * valueGreaterThan({ timestampValue: '2021-01-01T00:00:00.000Z' }, { timestampValue: '2021-01-02T00:00:00.000Z' }) // returns false
 */
export function valueGreaterThan(
  a: FirestoreValue,
  b: FirestoreValue,
): boolean {
  if (a.stringValue !== undefined && b.stringValue !== undefined) {
    return a.stringValue > b.stringValue;
  }
  if (a.integerValue !== undefined && b.integerValue !== undefined) {
    return Number(a.integerValue) > Number(b.integerValue);
  }
  if (a.doubleValue !== undefined && b.doubleValue !== undefined) {
    return Number(a.doubleValue) > Number(b.doubleValue);
  }
  const aTs = timestampToMs(a);
  const bTs = timestampToMs(b);
  if (aTs !== null && bTs !== null) {
    return aTs > bTs;
  }
  return false;
}

/**
 * Check if an array contains a value
 * @param fieldValue - The field value to check
 * @param compareValue - The value to check for
 * @returns True if the array contains the value, false otherwise
 * @example
 * arrayContains({ arrayValue: { values: [{ stringValue: 'apple' }] } }, { stringValue: 'apple' }) // returns true
 * arrayContains({ arrayValue: { values: [{ integerValue: '10' }] } }, { integerValue: '20' }) // returns false
 * arrayContains({ arrayValue: { values: [{ timestampValue: '2021-01-01T00:00:00.000Z' }] } }, { timestampValue: '2021-01-02T00:00:00.000Z' }) // returns false
 */
export function arrayContains(
  fieldValue: FirestoreValue,
  compareValue: FirestoreValue,
): boolean {
  if (!fieldValue.arrayValue || !fieldValue.arrayValue.values) {
    return false;
  }
  return fieldValue.arrayValue.values.some((val) =>
    valuesEqual(val, compareValue),
  );
}

/**
 * Check if a value is in an array
 * @param fieldValue - The field value to check
 * @param compareValue - The value to check for
 * @returns True if the value is in the array, false otherwise
 * @example
 * valueIn({ stringValue: 'apple' }, { arrayValue: { values: [{ stringValue: 'apple' }] } }) // returns true
 * valueIn({ integerValue: '10' }, { arrayValue: { values: [{ integerValue: '10' }] } }) // returns true
 * valueIn({ timestampValue: '2021-01-01T00:00:00.000Z' }, { arrayValue: { values: [{ timestampValue: '2021-01-02T00:00:00.000Z' }] } }) // returns false
 */
export function valueIn(
  fieldValue: FirestoreValue,
  compareValue: FirestoreValue,
): boolean {
  if (!compareValue.arrayValue || !compareValue.arrayValue.values) {
    return false;
  }
  return compareValue.arrayValue.values.some((val) =>
    valuesEqual(fieldValue, val),
  );
}

/**
 * Check if an array contains any value
 * @param fieldValue - The field value to check
 * @param compareValue - The value to check for
 * @returns True if the array contains any value, false otherwise
 * @example
 * arrayContainsAny({ arrayValue: { values: [{ stringValue: 'apple' }] } }, { arrayValue: { values: [{ stringValue: 'banana' }] } }) // returns true
 * arrayContainsAny({ arrayValue: { values: [{ integerValue: '10' }] } }, { arrayValue: { values: [{ integerValue: '20' }] } }) // returns true
 * arrayContainsAny({ arrayValue: { values: [{ timestampValue: '2021-01-01T00:00:00.000Z' }] } }, { arrayValue: { values: [{ timestampValue: '2021-01-02T00:00:00.000Z' }] } }) // returns false
 */
export function arrayContainsAny(
  fieldValue: FirestoreValue,
  compareValue: FirestoreValue,
): boolean {
  if (!fieldValue.arrayValue || !fieldValue.arrayValue.values) {
    return false;
  }
  if (!compareValue.arrayValue || !compareValue.arrayValue.values) {
    return false;
  }
  return fieldValue.arrayValue.values.some((val) =>
    compareValue.arrayValue!.values.some((compareVal) =>
      valuesEqual(val, compareVal),
    ),
  );
}

/**
 * Compare field value with operator (compareValue already normalized)
 * @param fieldValue - The field value to compare
 * @param operator - The operator to compare with
 * @param normalizedCompareValue - The normalized compare value
 * @returns True if the field value matches the operator, false otherwise
 * @example
 * compareFieldValueWithNormalized({ stringValue: 'apple' }, 'EQUAL', { stringValue: 'apple' }) // returns true
 * compareFieldValueWithNormalized({ integerValue: '10' }, 'EQUAL', { integerValue: '10' }) // returns true
 * compareFieldValueWithNormalized({ timestampValue: '2021-01-01T00:00:00.000Z' }, 'EQUAL', { timestampValue: '2021-01-01T00:00:00.000Z' }) // returns true
 */
export function compareFieldValueWithNormalized(
  fieldValue: FirestoreValue | undefined,
  operator: string,
  normalizedCompareValue: FirestoreValue,
): boolean {
  if (!fieldValue) {
    if (operator === 'EQUAL' || operator === '==') {
      return false;
    }
    if (operator === 'NOT_EQUAL' || operator === '!=') {
      return true;
    }
    return false;
  }
  switch (operator) {
    case 'EQUAL':
    case '==':
      return valuesEqual(fieldValue, normalizedCompareValue);
    case 'NOT_EQUAL':
    case '!=':
      return !valuesEqual(fieldValue, normalizedCompareValue);
    case 'LESS_THAN':
    case '<':
      return valueLessThan(fieldValue, normalizedCompareValue);
    case 'LESS_THAN_OR_EQUAL':
    case '<=':
      return (
        valueLessThan(fieldValue, normalizedCompareValue) ||
        valuesEqual(fieldValue, normalizedCompareValue)
      );
    case 'GREATER_THAN':
    case '>':
      return valueGreaterThan(fieldValue, normalizedCompareValue);
    case 'GREATER_THAN_OR_EQUAL':
    case '>=':
      return (
        valueGreaterThan(fieldValue, normalizedCompareValue) ||
        valuesEqual(fieldValue, normalizedCompareValue)
      );
    case 'ARRAY_CONTAINS':
      return arrayContains(fieldValue, normalizedCompareValue);
    case 'IN':
      return valueIn(fieldValue, normalizedCompareValue);
    case 'ARRAY_CONTAINS_ANY':
      return arrayContainsAny(fieldValue, normalizedCompareValue);
    default:
      return false;
  }
}

/**
 * Apply a field filter (e.g., field == value)
 * @param documents - The documents to filter
 * @param fieldFilter - The field filter to apply
 * @param logger - The logger to use for logging
 * @param reconstruct - The function to reconstruct the document fields
 * @returns The filtered documents
 * @example
 * applyFieldFilter([{ name: 'my-document' }], { field: 'name', op: 'EQUAL', value: 'my-document' }, logger, reconstruct) // returns [{ name: 'my-document' }]
 */
export function applyFieldFilter(
  documents: FirestoreDocument[],
  fieldFilter: any,
  logger: FirestoreLogger,
  reconstruct: (
    doc: FirestoreDocument,
    log?: FirestoreLogger,
  ) => Record<string, FirestoreValue>,
): FirestoreDocument[] {
  const field =
    fieldFilter.field ||
    fieldFilter.Field ||
    fieldFilter.field_reference ||
    fieldFilter.fieldReference;
  const op =
    fieldFilter.op ||
    fieldFilter.Op ||
    fieldFilter.operator ||
    fieldFilter.Operator;
  const value =
    fieldFilter.value ||
    fieldFilter.Value ||
    fieldFilter.value_value ||
    fieldFilter.valueValue;

  if (!field || !op || !value) {
    logger?.log(
      'grpc',
      `RunQuery DEBUG: Field filter missing required fields: field=${!!field}, op=${!!op}, value=${!!value}`,
    );
    return documents;
  }

  const fieldPath =
    field.field_path ||
    field.fieldPath ||
    field.field_path_string ||
    field.fieldPathString ||
    '';
  const operator = op.toUpperCase();

  let normalizedCompareValue: FirestoreValue;
  try {
    normalizedCompareValue = safeNormalizeFilterValue(value);
  } catch {
    normalizedCompareValue = { nullValue: null };
  }

  let filtered: FirestoreDocument[];
  try {
    filtered = documents.filter((doc) => {
      try {
        let fieldValue: FirestoreValue | undefined;
        if (fieldPath === '__name__') {
          const docName = doc.name;
          fieldValue =
            docName !== null && docName !== undefined && docName !== ''
              ? { referenceValue: docName }
              : undefined;
        } else {
          const docFields = reconstruct(doc, logger);
          fieldValue = getFieldValueByPath(docFields, fieldPath);
          if (
            fieldValue &&
            typeof fieldValue === 'object' &&
            Object.keys(fieldValue).length > 0
          ) {
            fieldValue = normalizeGrpcValueToFirestoreValue(fieldValue);
          }
        }
        return compareFieldValueWithNormalized(
          fieldValue,
          operator,
          normalizedCompareValue,
        );
      } catch {
        return false;
      }
    });
  } catch {
    filtered = [];
  }

  logger?.log(
    'grpc',
    `RunQuery DEBUG: Field filter result: ${filtered.length} of ${documents.length} documents match`,
  );
  return filtered;
}

/**
 * Apply a unary filter (IS_NULL, IS_NAN, etc.)
 * @param documents - The documents to filter
 * @param unaryFilter - The unary filter to apply
 * @param reconstruct - The function to reconstruct the document fields
 * @returns The filtered documents
 * @example
 * applyUnaryFilter([{ name: 'my-document' }], { op: 'IS_NULL', field: 'name' }, reconstruct) // returns [{ name: 'my-document' }]
 */
export function applyUnaryFilter(
  documents: FirestoreDocument[],
  unaryFilter: any,
  reconstruct: (
    doc: FirestoreDocument,
    log?: FirestoreLogger,
  ) => Record<string, FirestoreValue>,
): FirestoreDocument[] {
  const op =
    unaryFilter.op ||
    unaryFilter.Op ||
    unaryFilter.operator ||
    unaryFilter.Operator;
  const field =
    unaryFilter.field ||
    unaryFilter.Field ||
    unaryFilter.field_reference ||
    unaryFilter.fieldReference;

  if (!op || !field) {
    return documents;
  }

  const fieldPath =
    field.field_path ||
    field.fieldPath ||
    field.field_path_string ||
    field.fieldPathString ||
    '';
  const operator = op.toUpperCase();

  return documents.filter((doc) => {
    const docFields = reconstruct(doc);
    const fieldValue = getFieldValueByPath(docFields, fieldPath);

    if (operator === 'IS_NULL') {
      return (
        !fieldValue || fieldValue.nullValue !== undefined || fieldValue === null
      );
    }
    if (operator === 'IS_NAN') {
      if (!fieldValue || fieldValue.doubleValue === undefined) {
        return false;
      }
      const doubleVal =
        typeof fieldValue.doubleValue === 'string'
          ? parseFloat(fieldValue.doubleValue)
          : fieldValue.doubleValue;
      return typeof doubleVal === 'number' && isNaN(doubleVal);
    }
    if (operator === 'IS_NOT_NULL') {
      return (
        fieldValue && fieldValue.nullValue === undefined && fieldValue !== null
      );
    }
    return false;
  });
}

/**
 * Apply a composite filter (AND/OR of multiple filters)
 * @param documents - The documents to filter
 * @param compositeFilter - The composite filter to apply
 * @param logger - The logger to use for logging
 * @param applyQueryFiltersFn - The function to apply the query filters
 * @returns The filtered documents
 * @example
 * applyCompositeFilter([{ name: 'my-document' }], { op: 'AND', filters: [{ op: 'EQUAL', field: 'name', value: 'my-document' }] }, logger, applyQueryFilters) // returns [{ name: 'my-document' }]
 */
export function applyCompositeFilter(
  documents: FirestoreDocument[],
  compositeFilter: any,
  logger: FirestoreLogger,
  applyQueryFiltersFn: (
    docs: FirestoreDocument[],
    filter: any,
    log?: FirestoreLogger,
  ) => FirestoreDocument[],
): FirestoreDocument[] {
  const op =
    compositeFilter.op ||
    compositeFilter.Op ||
    compositeFilter.operator ||
    compositeFilter.Operator ||
    'AND';
  const filters =
    compositeFilter.filters ||
    compositeFilter.Filters ||
    compositeFilter.filter ||
    compositeFilter.Filter ||
    [];

  if (!Array.isArray(filters) || filters.length === 0) {
    return documents;
  }

  const operator = op.toUpperCase();

  if (operator === 'AND') {
    logger?.log(
      'grpc',
      `RunQuery DEBUG: Applying AND composite filter with ${filters.length} sub-filters`,
    );
    let result = documents;
    for (let i = 0; i < filters.length; i++) {
      const filter = filters[i];
      logger?.log(
        'grpc',
        `RunQuery DEBUG: Applying sub-filter ${i + 1}/${filters.length}: ${JSON.stringify(filter).substring(0, 200)}`,
      );
      const beforeCount = result.length;
      result = applyQueryFiltersFn(result, filter, logger);
      logger?.log(
        'grpc',
        `RunQuery DEBUG: Sub-filter ${i + 1} result: ${result.length} of ${beforeCount} documents match`,
      );
    }
    return result;
  }
  if (operator === 'OR') {
    const results: FirestoreDocument[] = [];
    for (const filter of filters) {
      const filtered = applyQueryFiltersFn(documents, filter, logger);
      for (const doc of filtered) {
        if (!results.find((d) => d.name === doc.name)) {
          results.push(doc);
        }
      }
    }
    return results;
  }
  return documents;
}

/**
 * Apply query filters to documents
 * @param documents - The documents to filter
 * @param filter - The filter to apply
 * @param logger - The logger to use for logging
 * @returns The filtered documents
 * @example
 * applyQueryFilters([{ name: 'my-document' }], { field: 'name', op: 'EQUAL', value: 'my-document' }, logger) // returns [{ name: 'my-document' }]
 */
export function applyQueryFilters(
  documents: FirestoreDocument[],
  filter: any,
  logger?: FirestoreLogger,
): FirestoreDocument[] {
  if (!filter || !documents.length) {
    return documents;
  }

  const fieldFilter =
    filter.field_filter || filter.fieldFilter || filter.FieldFilter;
  const compositeFilter =
    filter.composite_filter || filter.compositeFilter || filter.CompositeFilter;
  const unaryFilter =
    filter.unary_filter || filter.unaryFilter || filter.UnaryFilter;

  const reconstruct = (doc: FirestoreDocument, log?: FirestoreLogger) =>
    reconstructDocumentFields(doc, log);

  if (fieldFilter) {
    logger?.log(
      'grpc',
      `RunQuery DEBUG: Applying field filter: ${JSON.stringify(fieldFilter).substring(0, 300)}`,
    );
    return applyFieldFilter(documents, fieldFilter, logger, reconstruct);
  }
  if (compositeFilter) {
    logger?.log(
      'grpc',
      `RunQuery DEBUG: Applying composite filter: ${JSON.stringify(compositeFilter).substring(0, 500)}`,
    );
    const result = applyCompositeFilter(
      documents,
      compositeFilter,
      logger,
      applyQueryFilters,
    );
    logger?.log(
      'grpc',
      `RunQuery DEBUG: Composite filter result: ${result.length} of ${documents.length} documents match`,
    );
    return result;
  }
  if (unaryFilter) {
    return applyUnaryFilter(documents, unaryFilter, reconstruct);
  }
  return documents;
}

/**
 * Apply orderBy to documents (sort by field path and direction)
 * @param documents - The documents to order
 * @param orderBy - The order by to apply
 * @param reconstruct - The function to reconstruct the document fields
 * @returns The ordered documents
 * @example
 * applyOrderBy([{ name: 'my-document' }], [{ field: 'name', direction: 1 }], reconstruct) // returns [{ name: 'my-document' }]
 */
export function applyOrderBy(
  documents: FirestoreDocument[],
  orderBy: any[],
  reconstruct: (
    doc: FirestoreDocument,
    log?: FirestoreLogger,
  ) => Record<string, FirestoreValue>,
): FirestoreDocument[] {
  const arr = Array.isArray(orderBy) ? orderBy : Object.values(orderBy || {});
  if (arr.length === 0 || documents.length === 0) {
    return documents;
  }

  const orders = arr.map((o: any) => {
    const field = o.field || o.Field || o.field_reference || o.fieldReference;
    const path =
      field?.field_path ?? field?.fieldPath ?? field?.field_path_string ?? '';
    const dir = o.direction ?? o.Direction ?? 1;
    return { path, ascending: dir !== 2 };
  });

  return [...documents].sort((a, b) => {
    for (const { path, ascending } of orders) {
      const fieldsA = reconstruct(a);
      const fieldsB = reconstruct(b);
      const valA =
        path === '__name__' ? a.name : getFieldValueByPath(fieldsA, path);
      const valB =
        path === '__name__' ? b.name : getFieldValueByPath(fieldsB, path);
      const cmp = compareValuesForOrder(valA, valB);
      if (cmp !== 0) {
        return ascending ? cmp : -cmp;
      }
    }
    return 0;
  });
}

/**
 * Destroy a duplex stream with UNIMPLEMENTED (for Listen/Write stubs)
 * @param call - The duplex stream to destroy
 * @param details - The details of the error
 * @returns The destroyed duplex stream
 * @example
 * destroyStreamWithUnimplemented(call, 'UNIMPLEMENTED') // returns the destroyed duplex stream
 */
export function destroyStreamWithUnimplemented(
  call: grpc.ServerDuplexStream<any, any>,
  details: string,
): void {
  call.destroy(
    Object.assign(new Error(details), {
      code: grpc.status.UNIMPLEMENTED,
      details,
    }) as grpc.ServiceError,
  );
}

/**
 * Emit a visible warning (or throw) when an unimplemented RPC is called
 * @param rpcName - The name of the RPC that was called
 * @returns The emitted warning
 * @example
 * emitUnimplementedWarning('listen') // emits a warning
 */
export function emitUnimplementedWarning(rpcName: string): void {
  const mode =
    config.getString('logs.onUnimplemented', 'warn') === 'throw'
      ? 'throw'
      : 'warn';
  const msg = `[FIREBASE-MOCKER] RPC NOT IMPLEMENTED: ${rpcName}. This emulator does not support this operation.`;
  const hint = `Set firebaseMocker.addConfig({ logs: { onUnimplemented: 'warn' } }) to only log, or 'throw' to fail the process.`;
  if (mode === 'throw') {
    throw new Error(`${msg} ${hint}`);
  }
  process.stderr.write(`\n*** ${msg} ***\n${hint}\n\n`, () => {});
}
