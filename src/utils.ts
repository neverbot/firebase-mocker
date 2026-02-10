/**
 * Utility functions for Firestore operations
 */

import { FirestoreValue, FirestoreDocument } from './types';

/**
 * google.protobuf.NullValue.NULL_VALUE = 0.
 * We must use 0 (not null) when building gRPC Value for responses, because protobufjs
 * skips fields with value null when encoding, so the client would decode an empty Value {}.
 */
export const GRPC_NULL_VALUE = 0;

/** Firestore Value oneof keys: exactly one must be set for a valid Value in gRPC responses. */
const VALUE_TYPE_KEYS = [
  'nullValue',
  'booleanValue',
  'integerValue',
  'doubleValue',
  'timestampValue',
  'stringValue',
  'bytesValue',
  'referenceValue',
  'geoPointValue',
  'arrayValue',
  'mapValue',
];

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
  // Handle null or undefined
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
 * Normalize a gRPC value (snake_case or camelCase) to FirestoreValue (camelCase)
 * This is a recursive function that handles all value types including nested arrays and maps
 */
export function normalizeGrpcValueToFirestoreValue(value: any): FirestoreValue {
  if (!value || typeof value !== 'object') {
    return { nullValue: null };
  }

  // Check both snake_case and camelCase formats
  // protobufjs (when loaded from JSON) uses camelCase
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

  if ('timestamp_value' in value || 'timestampValue' in value) {
    return {
      timestampValue: value.timestamp_value || value.timestampValue,
    };
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
          values: arrayVal.values.map(normalizeGrpcValueToFirestoreValue),
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
        );
      });
      return { mapValue: { fields: normalizedFields } };
    }
    return { mapValue: { fields: {} } };
  }

  // If nothing matched, return nullValue
  return { nullValue: null };
}

/**
 * Convert FirestoreValue (camelCase) to gRPC value format
 * When protobufjs loads from JSON, it uses camelCase, so we return camelCase
 * This is a recursive function that handles all value types including nested arrays and maps
 */
export function toGrpcValue(firestoreValue: FirestoreValue): any {
  if (!firestoreValue || typeof firestoreValue !== 'object') {
    return { nullValue: GRPC_NULL_VALUE };
  }

  // Ensure we never send a Value that decodes to {} - client SDK throws "Unable to infer type value from '{}'"
  const hasExactlyOne =
    VALUE_TYPE_KEYS.filter((k) => k in firestoreValue).length === 1;
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
    return { timestampValue: firestoreValue.timestampValue };
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
 * Recursively ensure every Value in the tree has exactly one value-type key.
 * Replaces any object that decodes to {} (client throws "Unable to infer type value from '{}'") with nullValue.
 */
export function sanitizeGrpcValueForResponse(value: any): any {
  if (value === null || value === undefined) {
    return { nullValue: GRPC_NULL_VALUE };
  }
  if (typeof value !== 'object') {
    return value;
  }
  const valueKeyCount = VALUE_TYPE_KEYS.filter((k) => k in value).length;
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
 * Sanitize all field values in a gRPC fields map (recursively).
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
 * Convert FirestoreValue fields to gRPC format
 * When protobufjs loads from JSON, it uses camelCase, so we return camelCase
 */
export function toGrpcFields(
  fields: Record<string, FirestoreValue>,
): Record<string, any> {
  const grpcFields: Record<string, any> = {};
  Object.keys(fields).forEach((key) => {
    const grpcValue = toGrpcValue(fields[key]);
    // Only add if grpcValue has at least one property
    if (Object.keys(grpcValue).length > 0) {
      grpcFields[key] = grpcValue;
    }
  });
  return grpcFields;
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

/**
 * Convert Date to gRPC Timestamp format
 */
export function toTimestamp(date: Date): { seconds: number; nanos: number } {
  const ms = date.getTime();
  return {
    seconds: Math.floor(ms / 1000),
    nanos: (ms % 1000) * 1000000,
  };
}
