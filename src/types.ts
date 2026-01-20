/**
 * Type definitions for Firestore document structure
 */

export interface FirestoreDocument {
  name: string; // Full document path
  fields: Record<string, FirestoreValue>;
  createTime?: string;
  updateTime?: string;
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
