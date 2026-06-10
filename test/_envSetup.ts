/**
 * Mocha `require` hook that fixes Firebase emulator env vars BEFORE any
 * `firebase-admin` submodule is imported.
 *
 * Why: firebase-admin v14 ships modular submodules (e.g.
 * `firebase-admin/remote-config`) that read their host overrides at module
 * load time, not per-request. If a spec file imports the submodule before
 * `_setup.ts`'s `before()` hook has a chance to set the env var, the SDK
 * captures the production URL and the test hits Google.
 *
 * Setting the vars here guarantees they are present before any import in any
 * spec file executes — `_setup.ts` simply confirms the matching emulator is
 * running on those ports.
 */

if (!process.env.FIRESTORE_EMULATOR_HOST) {
  process.env.FIRESTORE_EMULATOR_HOST = 'localhost:3333';
}
if (!process.env.FIREBASE_AUTH_EMULATOR_HOST) {
  process.env.FIREBASE_AUTH_EMULATOR_HOST = 'localhost:9099';
}
if (!process.env.FIREBASE_STORAGE_EMULATOR_HOST) {
  process.env.FIREBASE_STORAGE_EMULATOR_HOST = 'localhost:9199';
}
if (!process.env.FIREBASE_REMOTE_CONFIG_URL_BASE) {
  process.env.FIREBASE_REMOTE_CONFIG_URL_BASE = 'http://localhost:9299';
}
