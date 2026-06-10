/**
 * Side-effect register module for Mocha/Node `--require` hooks.
 *
 * Sets the four Firebase emulator host env vars to their defaults so the
 * `firebase-admin` modular submodules (`firebase-admin/firestore`,
 * `firebase-admin/auth`, `firebase-admin/storage`,
 * `firebase-admin/remote-config`) pick them up at module-load time — before
 * any test file imports the SDK.
 *
 * Why: `firebase-admin` v14 removed the legacy `admin.remoteConfig()` lazy
 * namespace. Consumers must now `import { getRemoteConfig } from
 * 'firebase-admin/remote-config'`, which evaluates a `const URL_BASE =
 * process.env.FIREBASE_REMOTE_CONFIG_URL_BASE || 'https://...'` at import
 * time. Calling `firebaseMocker.startRemoteConfigServer()` inside a Mocha
 * `before()` hook is therefore too late — the constant has already
 * captured the production URL.
 *
 * Usage (mocharc):
 *   {
 *     "require": [
 *       "ts-node/register/transpile-only",
 *       "firebase-mocker/register"
 *     ]
 *   }
 *
 * Each var is set only if not already defined, so a consumer running on
 * non-default ports can `process.env.FIREBASE_REMOTE_CONFIG_URL_BASE =
 * 'http://localhost:9300'` in their own pre-require file and this module
 * will leave that override intact.
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
