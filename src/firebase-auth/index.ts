/**
 * Firebase Auth emulator: REST API compatible with firebase-admin when
 * FIREBASE_AUTH_EMULATOR_HOST is set.
 */

export { AuthServer } from './server';
export type { AuthServerConfig } from './server';
export { AuthStorage, AuthEmulatorUser } from './storage';
export type { OobRequestType, OobCodeEntry } from './storage';
export { generateTestIdToken } from './jwt';
export type { TestIdTokenOptions } from './jwt';
