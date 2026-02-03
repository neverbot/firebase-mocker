/**
 * Type definitions for Firestore document structure
 */

export type FieldType =
  | 'nullValue'
  | 'booleanValue'
  | 'integerValue'
  | 'doubleValue'
  | 'timestampValue'
  | 'stringValue'
  | 'bytesValue'
  | 'referenceValue'
  | 'geoPointValue'
  | 'arrayValue'
  | 'mapValue';

export interface FirestoreDocument {
  name: string; // Full document path
  fields: Record<string, FirestoreValue>;
  createTime?: string;
  updateTime?: string;
  // Metadata about field types - used to reconstruct values when proto-loader loses oneof data
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
