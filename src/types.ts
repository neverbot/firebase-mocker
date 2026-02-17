/**
 * Type definitions for Firestore document structure
 */

/** All Firestore value type keys (single source of truth for runtime checks). */
export const FIELD_TYPE_KEYS = [
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
] as const;

export type FieldType = (typeof FIELD_TYPE_KEYS)[number];

export interface FirestoreDocument {
  name: string; // Full document path
  fields: Record<string, FirestoreValue>;
  createTime?: string;
  updateTime?: string;
  // Metadata about field types - used to preserve field type information
  fieldTypes?: Record<string, FieldType>;
}

export interface FirestoreValue {
  nullValue?: null;
  booleanValue?: boolean;
  integerValue?: string;
  doubleValue?: number;
  timestampValue?: string;
  stringValue?: string;
  bytesValue?: string;
  referenceValue?: string;
  geoPointValue?: {
    latitude: number;
    longitude: number;
  };
  arrayValue?: {
    values: FirestoreValue[];
  };
  mapValue?: {
    fields: Record<string, FirestoreValue>;
  };
}

export type FirestoreCollection = Record<string, FirestoreDocument>;

export type FirestoreDatabase = Record<string, FirestoreCollection>;

export type FirestoreProject = Record<string, FirestoreDatabase>;

export interface ServerConfig {
  port: number;
  host: string;
  projectId?: string;
}
