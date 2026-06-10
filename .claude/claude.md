# Firebase Mocker

## Overview

`firebase-mocker` is a TypeScript package that runs **local in-process emulators of Firebase services**. It is designed to be launched from code (e.g. from a backend's test setup) and exposes one or more local servers on `localhost`. Once started, it sets the standard Firebase emulator environment variables so that an unmodified Firebase Admin SDK in the same process talks to these local servers instead of real Firebase.

The goal is **fast, hermetic testing**: no network, no real Firebase project, no credentials — but the application code under test keeps using the regular `firebase-admin` APIs.

Currently emulated:

- **Firestore** — gRPC server (default port `3333`), sets `FIRESTORE_EMULATOR_HOST`.
- **Firebase Auth** — HTTP server implementing the Identity Toolkit REST API (default port `9099`), sets `FIREBASE_AUTH_EMULATOR_HOST`.
- **Firebase Storage** — HTTP server implementing the Google Cloud Storage JSON API (default port `9199`), sets `FIREBASE_STORAGE_EMULATOR_HOST`.
- **Firebase Remote Config** — HTTP server implementing the Remote Config REST API (default port `9299`), sets `FIREBASE_REMOTE_CONFIG_URL_BASE`.

Each server can be started independently or in any combination. State is kept in memory; nothing is persisted to disk.

## Public API (entry point)

The package exposes a `firebaseMocker` object from `src/index.ts`:

- `firebaseMocker.startFirestoreServer({ port, host, projectId })` — start the Firestore gRPC emulator. Sets `FIRESTORE_EMULATOR_HOST` before returning.
- `firebaseMocker.stopFirestoreServer()` — stop it.
- `firebaseMocker.startAuthServer({ port, host, projectId })` — start the Auth HTTP emulator. Sets `FIREBASE_AUTH_EMULATOR_HOST` before returning.
- `firebaseMocker.stopAuthServer()` — stop it.
- `firebaseMocker.startStorageServer({ port, host, projectId })` — start the Storage HTTP emulator. Sets `FIREBASE_STORAGE_EMULATOR_HOST` before returning.
- `firebaseMocker.stopStorageServer()` — stop it.
- `firebaseMocker.startRemoteConfigServer({ port, host, projectId, initialTemplate? })` — start the Remote Config HTTP emulator. Sets `FIREBASE_REMOTE_CONFIG_URL_BASE` before returning.
- `firebaseMocker.stopRemoteConfigServer()` — stop it (restores previous `FIREBASE_REMOTE_CONFIG_URL_BASE`).

Side-effect register hook (since v2.0.0):

- `firebase-mocker/register` — sub-export that, when required, sets `FIRESTORE_EMULATOR_HOST`, `FIREBASE_AUTH_EMULATOR_HOST`, `FIREBASE_STORAGE_EMULATOR_HOST`, and `FIREBASE_REMOTE_CONFIG_URL_BASE` to their defaults (only if not already set). Required for projects on `firebase-admin` v14+ that import `firebase-admin/remote-config` because that submodule caches the URL constant at module-load time. Add to mocharc `require` or pass via `NODE_OPTIONS='--require firebase-mocker/register'`. Source: `src/register.ts`. Compiled output: `dist/register.js`. Exposed through the `exports` field in `package.json`.
- `addConfig({ logs: { verboseGrpcLogs, verboseAuthLogs, verboseStorageLogs, onUnimplemented } })` — configure logging and the policy for unimplemented RPCs (`'warn'` default, or `'throw'` for strict CI).

All `start*Server` methods must be called **before** `admin.initializeApp(...)` so the env vars are picked up by the Admin SDK.

## Repo layout

- `src/` — TypeScript sources
  - `src/index.ts` — public API (`firebaseMocker`)
  - `src/firestore/` — Firestore gRPC server, handlers, in-memory storage, query/value conversion
  - `src/firebase-auth/` — Firebase Auth HTTP server (Identity Toolkit endpoints) and in-memory user store
  - `src/firebase-storage/` — Firebase Storage HTTP server (GCS JSON API) and in-memory object store
  - `src/firebase-remote-config/` — Firebase Remote Config HTTP server and in-memory template store
  - `src/config.ts`, `src/logger.ts`, `src/types.ts` — shared config, logging, types
- `proto/v1.json` — Firestore proto definitions, copied from `@google-cloud/firestore/build/protos/v1.json`
- `test/` — Mocha + Chai test suites (`firestore.spec.ts`, `firestore-server.spec.ts`, `firestore-utils.spec.ts`, `firebase-auth.spec.ts`, `firebase-storage.spec.ts`, `firebase-remote-config.spec.ts`, plus `firestore/`, `firebase-auth/`, `firebase-storage/`, and `firebase-remote-config/` helpers and `_setup.ts`)
- `scripts/` — helper scripts
- `dist/` — build output (`tsc`)
- `readme.md` — user-facing documentation
- `package.json`, `tsconfig.json`, `eslint.config.mjs` — tooling

## Build, test, lint

- `npm run build` — `tsc` build into `dist/`
- `npm run dev` — `ts-node src/index.ts`
- `npm test` — `NODE_ENV=test npx c8 mocha` (Mocha + Chai under c8 coverage)
- `npm run test:only` — Mocha without coverage
- `npm run lint` / `lint:fix` / `lint:quiet` — ESLint

## Firestore emulator (gRPC)

The Firestore server uses **gRPC** (not REST), loaded from `proto/v1.json` (the same protos shipped by `@google-cloud/firestore`). It binds to `[::]:port` so both IPv4 and IPv6 connections are accepted (the Firebase Admin SDK may attempt IPv6).

### Implemented gRPC methods

- `GetDocument`
- `ListDocuments`
- `RunQuery` (with `where` field/composite/unary filters, `orderBy`, `limit`, `offset`, and cursor pagination `startAt` / `startAfter` / `endAt` / `endBefore`)
- `RunAggregationQuery` (COUNT supported; sum/avg currently return 0)
- `CreateDocument`
- `UpdateDocument`
- `DeleteDocument`
- `Commit` (used by `set()`, `add()`, `update()`, `delete()`, plus `FieldValue` transforms — see below)
- `BatchGetDocuments` (used by `doc.get()`)
- `Listen` (real-time listeners, streaming)
- `Write` (write stream used by client SDK)
- `ListCollectionIds` (`doc.ref.listCollections()`)
- `BeginTransaction` (Level 1: atomic commit, no conflict detection)
- `Rollback` (Level 1: removes transaction tracking)
- `BatchWrite` (per-write status, non-atomic; reuses Commit per write)

### Not implemented (return UNIMPLEMENTED)

_(none — every gRPC method exposed by the proto is wired)_

When an unimplemented RPC is called, the server logs a clear warning to stderr (or throws if `logs.onUnimplemented === 'throw'`).

### Dotted-path updates (`update_mask`)

`ref.update({ 'a.b': v })` is encoded by the Admin SDK as a Write with `update_mask.field_paths = ['a.b']` and a nested map in `update.fields`. The `Commit` handler detects dotted paths in the mask and, instead of replacing the top-level field, walks each path through the nested `mapValue.fields` and applies only the leaf change. Helpers in `src/firestore/handlers/commit.ts`:

- `getValueAtPath(fields, path)` — walks dotted path through nested `mapValue.fields`, returns leaf or `undefined`.
- `setValueAtPath(fields, path, value)` — creates intermediate empty `mapValue` containers as needed.
- `deleteValueAtPath(fields, path)` — removes the leaf; no-op if intermediate is missing.

When the mask contains at least one dotted path, the entire merge takes the dotted-path branch (deep clone of existing fields, then walk every masked path). `FieldValue.delete()` along a dotted path is just "path absent from incoming `fields`" → `deleteValueAtPath`. Non-dotted masks fall back to the original top-level merge.

### `FieldValue` transforms

The `Commit` handler inspects both `write.updateTransforms` (modern SDK path) and `write.transform.fieldTransforms` (legacy) and applies these sentinels alongside the regular `update.fields` payload:

- `serverTimestamp()` — written as `timestampValue` using `new Date().toISOString()`.
- `increment(n)` — adds `n` to `existingDoc.fields[fieldPath]` (treats missing/non-numeric as 0). Result kind is `integerValue` only when both base and operand are int; any `doubleValue` yields a `doubleValue`. Implemented in `applyFieldTransform()` inside `src/firestore/handlers/commit.ts`.
- `maximum` / `minimum` — same numeric logic as `increment`, using `Math.max` / `Math.min`.
- `arrayUnion(...values)` (proto: `appendMissingElements`) — appends values not already present using deep equality after stripping the protobufjs `valueType` discriminator (`canonicalizeValue()`).
- `arrayRemove(...values)` (proto: `removeAllFromArray`) — removes every occurrence matching one of the given values.

Comparison of array elements goes through `canonicalizeValue()` because the SDK encodes incoming Values with an extra `valueType` field that the in-memory store does not carry; without stripping it, `arrayUnion` would re-append duplicates. Stored values are canonicalized too so subsequent reads stay stable.

### `allDescendants` queries (collectionGroup, recursiveDelete)

`RunQuery` honors `from[].all_descendants = true`. Two flavors are detected:

- **collectionGroup** (`db.collectionGroup('X').get()`): the `from` selector carries `collectionId = 'X'` with `allDescendants = true`. The handler walks the whole database via `FirestoreStorage.listAllDocumentsWithPath()` and keeps docs whose stored `collectionPath` has `X` as its last segment (the leaf collection).

- **Kindless** (used by `firestore.recursiveDelete()`): the SDK omits `collectionId` and encodes the scope as a `__name__` range filter (`>= path/MIN_ID` and `< path\0/MIN_ID`). Because these filter values reference full document paths with a zero-byte separator, the generic field-filter pipeline can't apply them correctly. The handler detects this shape (`whereHasNameFilter`), extracts the root collection segment from the filter value (`extractKindlessRootCollection`), and turns it into a path-prefix scan; the original `where` clause is skipped so the prefix scan does all the scoping.

Both flavors return documents using their stored `name` (full path), since `allDescendants` queries match docs across many collections — the response can't be rebuilt from the request's single `collectionPath`.

The storage layer is unchanged: the existing flat-keyed map `database[collectionPath]` already encodes hierarchy as path prefixes, so descendant lookups are a single linear scan over `Object.keys`. No secondary index is maintained.

### BatchWrite

`BatchWrite` is implemented in `src/firestore/handlers/batchWrite.ts` and powers `db.bulkWriter()`. Each input Write is fed through `handleCommit` individually (via a synthetic single-write call object), and the per-write outcome is translated into a `{writeResults, status}` response. Notes:

- Response uses **camelCase** (`writeResults`, `updateTime`) because the SDK's `BulkWriter` reads `response.writeResults[i].updateTime` directly. This contrasts with `CommitResponse` which uses snake_case — see the field naming notes in this file.
- A failed Write becomes `{writeResult: {}, status: {code: <grpc>, message}}` in the same index; the rest still succeed. The batch as a whole is non-atomic by design.
- `firestore.recursiveDelete()` works end-to-end: the kindless descendant query is handled by `RunQuery` (see the `allDescendants` section above) and the deletes flow through `BatchWrite`.

### Transactions (Level 1 semantics)

`db.runTransaction()` works for atomic commits in single-threaded test scenarios but does not detect conflicts:

- `BeginTransaction` generates a UUID transaction ID, tracked in `FirestoreStorage.activeTransactions`.
- `BatchGetDocuments` and `RunQuery` accept `newTransaction` (generates implicit txn ID, returned in first response) or `transaction` (existing txn, no-op).
- `Commit` accepts the `transaction` field and removes the ID from tracking after writes are applied.
- `Rollback` removes the ID from tracking and returns success.
- No read set tracking, no document versioning, no `ABORTED` retry path.

For tests that need real isolation semantics, use production or the official Firebase emulator.

## Firebase Auth emulator (HTTP)

The Auth server implements a subset of the Identity Toolkit REST API under `/identitytoolkit.googleapis.com/v1/projects/:projectId/...`, in memory. The Firebase Admin Auth client uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set.

Implemented endpoints:

- `accounts:lookup` — `getUser(uid)`, `getUserByEmail(email)`, `getUserByPhoneNumber(phone)`
- `accounts` (POST) — `createUser({ email, password, ... })`
- `accounts:delete` — `deleteUser(uid)`
- `accounts:update` — `updateUser(uid, { ... })`
- `accounts:sendOobCode` — `generatePasswordResetLink(email)` (returns `{email, oobCode, oobLink}`)
- `accounts:signInWithCustomToken` — decodes the JWT (no signature check), extracts `uid`/`sub` (and optional `claims`), mints an emulator idToken via `generateTestIdToken`, auto-creates the user if missing. Returns `{idToken, refreshToken, expiresIn, localId, isNewUser}`.

Other Identity Toolkit endpoints (email/password sign-in, phone sign-in, IDP, email link confirmation, etc.) return 404.

### Test ID token helper

`firebaseMocker.generateTestIdToken({uid, email, projectId})` produces an unsigned JWT (`alg: 'none'`) that the Firebase Admin SDK accepts via `verifyIdToken()` in emulator mode. Use this in tests to simulate authenticated requests. The user must exist in the auth emulator (call `auth.createUser({uid})` first) because the SDK internally calls `getUser(sub)` during verification.

## Firebase Storage emulator (HTTP)

The Storage server implements the Google Cloud Storage JSON API. The `@google-cloud/storage` client (used by `firebase-admin`) connects to it when `FIREBASE_STORAGE_EMULATOR_HOST` is set. `firebase-admin` internally converts this to `STORAGE_EMULATOR_HOST=http://host:port`.

### Implemented endpoints

- **Resumable upload** — `POST /upload/storage/v1/b/{bucket}/o` (create session) + `PUT` (upload data)
- **Download** — `GET /b/{bucket}/o/{name}?alt=media`
- **Get metadata** — `GET /b/{bucket}/o/{name}`
- **Update metadata** — `PATCH /b/{bucket}/o/{name}`
- **Delete** — `DELETE /b/{bucket}/o/{name}`
- **List objects** — `GET /b/{bucket}/o` (with `prefix`, `delimiter`, `maxResults`, `pageToken`)
- **Firebase download URL** — `GET /v0/b/{bucket}/o/{name}?alt=media&token=...`

### In-memory storage

Files are stored as `Buffer` objects in a `Map<bucket, Map<objectPath, { data, metadata }>>`. Metadata includes `name`, `bucket`, `contentType`, `size`, `timeCreated`, `updated`, `generation`, `md5Hash`, `crc32c`, and optional custom metadata. CRC32C is computed correctly (Castagnoli) so the SDK's upload integrity validation passes.

### Signed URLs (`getSignedUrl`)

`@google-cloud/storage`'s `file.getSignedUrl()` signs URLs locally using a service account private key, which the emulator does not have. `startStorageServer()` monkey-patches `File.prototype.getSignedUrl` to return a URL pointing to the emulator's existing `/b/:bucket/o/:file?alt=media` route. The patch is restored by `stopStorageServer()`. See `src/firebase-storage/sign-url-patch.ts`.

## Firebase Remote Config emulator (HTTP)

The Remote Config server implements a subset of the Firebase Remote Config REST API in memory. The Firebase Admin Remote Config client uses it when `FIREBASE_REMOTE_CONFIG_URL_BASE` is set (the SDK reads `process.env.FIREBASE_REMOTE_CONFIG_URL_BASE` and defaults to `https://firebaseremoteconfig.googleapis.com`).

Implemented endpoints:

- `GET /v1/projects/:projectId/remoteConfig` — `remoteConfig().getTemplate()`, returns body + `ETag` header
- `PUT /v1/projects/:projectId/remoteConfig` — `remoteConfig().publishTemplate(template)` with required `If-Match` header; returns 412 on mismatch, accepts `If-Match: *` to force

Out-of-scope endpoints (return 501 + warning via `logs.onUnimplemented`):

- `POST /v1/projects/:projectId/remoteConfig:rollback`
- `GET /v1/projects/:projectId/remoteConfig:listVersions`
- `GET /v1/projects/:projectId/namespaces/firebase-server/serverRemoteConfig`
- `PUT /v1/projects/:projectId/remoteConfig?validate_only=true`

### Etag strategy

Etags are strings of the form `etag-{projectId}-{counter}` where `counter` starts at 0 (or 1 if `initialTemplate` is provided) and increments on each successful publish. `version.versionNumber` mirrors the counter. No version history is retained; each publish overwrites the previous state.

### Scope

Designed for the `getTemplate()` + `publishTemplate(template)` flow only. There is no condition evaluation, no rollback, no version listing, no validateOnly support. Tests that need those features must run against production or the official Firebase emulator.

## Important technical notes

### Proto source

The Firestore server loads its definitions from `proto/v1.json`, which is a copy of `node_modules/@google-cloud/firestore/build/protos/v1.json`. To refresh after upgrading `firebase-admin` / `@google-cloud/firestore`:

```bash
cp node_modules/@google-cloud/firestore/build/protos/v1.json proto/v1.json
```

When adding or fixing gRPC handlers, compare against the official proto files in:

- `node_modules/@google-cloud/firestore/build/protos/google/firestore/v1/firestore.proto`
- `.../write.proto`, `.../document.proto`, `.../query.proto`

Match field numbers, field order, and types exactly.

### Field naming: snake_case vs camelCase

When using protobufjs with the JSON proto, field names must usually be **camelCase** (because the JSON proto carries `jsonName` mappings). However, the Firebase Admin SDK is inconsistent across paths. The current, **working** convention is:

- **CommitResponse** — `snake_case` (`write_results`, `commit_time`). **Do not change.** Document creation breaks otherwise.
- **WriteResult** — `snake_case` (`update_time`). **Do not change.**
- **BatchGetDocumentsResponse** — `camelCase` (`readTime`). Required for proper deserialization on read.
- **Document** — `camelCase` (`createTime`, `updateTime`). Required for proper deserialization on read.

If you change any of these, you must test **both** document creation and document reading; the typical failure mode is `Called 'readTime' on a local document` from the Admin SDK.

### Null values in gRPC `Value` messages (`GRPC_NULL_VALUE`)

When building Firestore `Value` messages for gRPC **responses** (BatchGetDocuments, GetDocument, RunQuery, etc.), never use `nullValue: null`. Use `nullValue: 0` (the proto enum value for `google.protobuf.NullValue.NULL_VALUE`).

- protobufjs **drops fields whose value is `null`** when encoding. So `{ nullValue: null }` is omitted from the wire entirely.
- The Firebase Admin SDK then decodes a `Value` with no oneof set, an empty `{}`, and throws: `Unable to infer type value from '{}'`.
- The constant `GRPC_NULL_VALUE = 0` is defined in `src/firestore/utils.ts` and used by `toGrpcValue()` and `sanitizeGrpcValueForResponse()`.
- The emulator's **internal** in-memory representation may still use `nullValue: null`. Only objects passed to protobufjs `fromObject()` for response serialization must use `0`.

### Do not patch firebase-admin

The emulator must work with an **unmodified** `firebase-admin`. Any compatibility fix must live inside `firebase-mocker`.

## Code style and conventions

- Code comments: English only.
- Markdown documentation: English only.
- Markdown filenames: lowercase (e.g. `readme.md`, not `README.md`).
- TypeScript throughout `src/`, ESLint + Prettier enforced via `eslint.config.mjs`.

## Git & Commits

- **Before every commit**, run and make sure all of these pass:
  1. `npm run lint:fix` — fix any remaining lint issues by hand
  2. `npm run build`
  3. `npm run test`
  Only commit once lint, build and tests are all green.
- Follow [Conventional Commits](https://www.conventionalcommits.org/).
- Commit messages must be a **single line only** — no body, no description paragraph, no blank lines, no `Co-Authored-By` or any other trailers.
- When a commit covers multiple unrelated topics, **split into separate commits** — one per topic.
- **Never push** — only commit locally. The user handles pushing to the remote.

## Possible future work

- `BatchWrite`
- Transactions Level 2: conflict detection and `ABORTED` retry semantics
- Optional persistence to disk
- Security rules emulation
- Additional Identity Toolkit endpoints (custom tokens, email links, etc.)
- Storage: copy/rewrite, compose objects
- Remote Config: `validate_only` dry-run, `rollback`, `listVersions`, server-side condition evaluation
