/**
 * Firebase Storage emulator: HTTP API compatible with firebase-admin when
 * STORAGE_EMULATOR_HOST is set.
 */

export { StorageServer } from './server';
export type { StorageServerConfig } from './server';
export { StorageStorage } from './storage';
export type { StoredObject, StoredObjectMetadata } from './storage';
export { patchGetSignedUrl, unpatchGetSignedUrl } from './sign-url-patch';
