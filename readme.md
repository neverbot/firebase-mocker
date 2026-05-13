# Firebase Mocker

A TypeScript-based emulator of the Firebase services. It provides **separate servers**. The Firebase Admin SDK connects to these local servers when the corresponding emulator environment variables are set.

 - **Firestore** (gRPC)
 - **Firebase Auth** (HTTP)
 - **Firebase Storage** (HTTP)
 - **Firebase Remote Config** (HTTP)

All data is stored **in memory** using TypeScript data structures (Maps, arrays, plain objects). Writes and reads go against these in-memory collections instead of a real database, so the emulator behaves like a real Firebase backend — but without network calls, credentials, or disk persistence.

## Overview

Firebase Mocker can run:

1. **Firestore emulator** — A **gRPC server** that implements the Firestore service contract. The Admin SDK talks to it when `FIRESTORE_EMULATOR_HOST` is set. Use it for local development, integration tests, and CI without real Firestore credentials.

2. **Firebase Auth emulator** — An **HTTP server** that implements the Identity Toolkit REST API. The Admin SDK Auth API (e.g. `getUserByEmail`, `createUser`, `deleteUser`) uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set. Use it to test auth flows without hitting production Firebase Auth.

3. **Firebase Storage emulator** — An **HTTP server** that implements the Google Cloud Storage JSON API. The Admin SDK Storage API (e.g. `bucket.file().save()`, `file.download()`, `file.delete()`) uses it when `FIREBASE_STORAGE_EMULATOR_HOST` is set. Use it to test file uploads and downloads without a real GCS bucket.

4. **Firebase Remote Config emulator** — An **HTTP server** that implements the Remote Config REST API. The Admin SDK Remote Config API (e.g. `remoteConfig().getTemplate()`, `remoteConfig().publishTemplate(template)`) uses it when `FIREBASE_REMOTE_CONFIG_URL_BASE` is set. Use it to test remote config reads and writes without a real Firebase project.

You can start **one, two, three, or all four** servers in the same process (e.g. in your test setup). Each server is independent: start only what your tests or app need.

## Installation

Install as a devDependency in your project (recommended for tests and local development):

```bash
npm install -D firebase-mocker
```

Or add to your project's `package.json`:

```json
{
  "devDependencies": {
    "firebase-mocker": "^1.0.0"
  }
}
```

Then run `npm install`.

### Development setup

```bash
npm install
npm run build
```

## Configuration

Configuration is passed when starting each server. You can pass different options to `startFirestoreServer()`, `startAuthServer()`, `startStorageServer()`, and `startRemoteConfigServer()`.

### Firestore server options

When calling `startFirestoreServer(config)`:

- **port** — gRPC server port (default `3333`)
- **host** — Bind address (e.g. `'localhost'`, or `'0.0.0.0'` for all interfaces)
- **projectId** — Project ID (must match the one used in your Firebase Admin app)

### Firebase Auth server options

When calling `startAuthServer(config)`:

- **port** — HTTP server port (default `9099`)
- **host** — Bind address (default `'localhost'`)
- **projectId** — Project ID (optional)

### Firebase Storage server options

When calling `startStorageServer(config)`:

- **port** — HTTP server port (default `9199`)
- **host** — Bind address (default `'localhost'`)
- **projectId** — Project ID (optional)

### Firebase Remote Config server options

When calling `startRemoteConfigServer(config)`:

- **port** — HTTP server port (default `9299`)
- **host** — Bind address (default `'localhost'`)
- **projectId** — Project ID (optional)
- **initialTemplate** — Optional initial template object (parameters, conditions, etc.) loaded before any test runs.

### Common options

For logs, use `addConfig({ logs: { ... } })` **before** starting servers:

- **verboseGrpcLogs** — Log every gRPC call (default `false`). Set `true` for debugging.
- **verboseAuthLogs** — Log Auth API requests (default `false`).
- **verboseStorageLogs** — Log Storage API requests (default `false`).
- **onUnimplemented** — When an RPC is not implemented: `'warn'` (default) writes a clear message to stderr and returns UNIMPLEMENTED; `'throw'` throws so the process fails. Example: `addConfig({ logs: { onUnimplemented: 'throw' } })` for strict CI.

## Usage

Start only the servers you need (one or both) **before** initializing the Firebase Admin SDK. Each `start*` call sets the matching emulator environment variable so the Admin SDK transparently routes to the local server.

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

// Sets FIRESTORE_EMULATOR_HOST with the right value automatically
const firestoreServer = await firebaseMocker.startFirestoreServer({
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
});

// Sets FIREBASE_AUTH_EMULATOR_HOST with the right value automatically
const authServer = await firebaseMocker.startAuthServer({
  port: 9099,
  host: 'localhost',
  projectId: 'my-project',
});

// Sets FIREBASE_STORAGE_EMULATOR_HOST with the right value automatically
const storageServer = await firebaseMocker.startStorageServer({
  port: 9199,
  host: 'localhost',
  projectId: 'my-project',
});

// Sets FIREBASE_REMOTE_CONFIG_URL_BASE with the right value automatically.
// `initialTemplate` is optional — preload parameters/conditions before tests run.
const remoteConfigServer = await firebaseMocker.startRemoteConfigServer({
  port: 9299,
  host: 'localhost',
  projectId: 'my-project',
  initialTemplate: {
    parameters: {
      welcome_msg: { defaultValue: { value: 'Hi' }, valueType: 'STRING' },
    },
  },
});

// Initialize Firebase Admin — it will pick up the emulator env vars
admin.initializeApp({ projectId: 'my-project' });
const db = admin.firestore();
const auth = admin.auth();
const bucket = admin.storage().bucket('my-bucket');

// Use the SDKs as usual; all calls go to the local emulators
await db.collection('users').doc('user1').set({ name: 'Jane' });
const user = await auth.createUser({ email: 'jane@example.com', password: 'secret' });
await bucket.file('hello.txt').save(Buffer.from('Hello!'), { contentType: 'text/plain' });
const template = await admin.remoteConfig().getTemplate();

// Teardown (e.g. after tests)
await firebaseMocker.stopFirestoreServer();
await firebaseMocker.stopAuthServer();
await firebaseMocker.stopStorageServer();
await firebaseMocker.stopRemoteConfigServer();
```

Notes:

- Call `start*Server()` **before** `admin.initializeApp(...)` so the emulator env vars are set in time.
- Each server is independent — call only the one(s) you need; `stop*` only the ones you started.
- Use `server.getStorage()` on any server instance to inspect the in-memory state from tests.

### Generating test ID tokens

For code that calls `admin.auth().verifyIdToken(token)` (e.g. auth middleware), use `generateTestIdToken` to produce a token that the Admin SDK will accept in emulator mode:

```typescript
import { firebaseMocker } from 'firebase-mocker';

const idToken = firebaseMocker.generateTestIdToken({
  uid: 'user-123',
  email: 'test@example.com',
  projectId: 'my-project', // must match admin.initializeApp({ projectId })
});

// Use in tests: e.g. as Authorization: Bearer ${idToken}
const decoded = await admin.auth().verifyIdToken(idToken);
// decoded.uid === 'user-123'
```

The token is an unsigned JWT (`alg: 'none'`), matching the format the official Firebase Auth Emulator produces. The Firebase Admin SDK in emulator mode validates `iss`, `aud`, `sub`, and `exp` — but skips signature verification.

> **Note**: The Admin SDK calls `getUser(sub)` internally when verifying tokens, so the user must exist in the auth emulator before `verifyIdToken` is called. Create the user via `auth.createUser({ uid })` first.

## Implemented APIs & Current Status

### Firestore (gRPC)

The Firestore emulator implements these gRPC methods:

| Method | Supported | Used by Firebase Admin SDK for |
|--------|-----------|---------------------------------|
| `GetDocument` | Yes | Single document fetch (`doc(id).get()`) |
| `ListDocuments` | Yes | List documents in a collection |
| `RunQuery` | Yes | Queries (`collection.get()`, `where()` field/composite/unary, `orderBy()` ASC/DESC including `__name__`, `limit()`, `offset()`, cursor pagination `startAt()` / `startAfter()` / `endAt()` / `endBefore()`, and `allDescendants` for `collectionGroup()` and `recursiveDelete()`) |
| `RunAggregationQuery` | Yes | Aggregation queries, e.g. `count().get()` (COUNT supported; sum/avg return 0) |
| `CreateDocument` | Yes | Create document with server-generated ID |
| `UpdateDocument` | Yes | Update existing document |
| `DeleteDocument` | Yes | Delete document |
| `Commit` | Yes | Writes: `set()`, `add()`, `update()`, `delete()`, plus `FieldValue` transforms (see below) |
| `BatchGetDocuments` | Yes | Batched reads, e.g. `doc(id).get()` |
| `Listen` | Yes | Real-time listeners (streaming) |
| `Write` | Yes | Write stream (used by client SDK) |
| `ListCollectionIds` | Yes | List subcollection IDs under a document (`doc.ref.listCollections()`) |
| `BatchWrite` | Yes | Used by `db.bulkWriter()` — per-write status, partial success allowed (non-atomic) |
| `BeginTransaction` | Yes (Level 1) | `db.runTransaction()` — atomic commit, no conflict detection (see note below) |
| `Rollback` | Yes | `db.runTransaction()` rollback when callback throws |

**Note on dotted-path updates:** `ref.update({ 'a.b': value })` is honored: when the SDK sends an `update_mask` with dotted paths, the emulator deep-merges only the masked paths into the stored document, preserving sibling fields inside the same map. `FieldValue.delete()` on a dotted path removes only that nested key. Top-level (non-dotted) paths still replace the whole field, matching real Firestore semantics.

**Note on `FieldValue` transforms:** The `Commit` handler applies the following sentinel transforms encoded by the Admin SDK on `set()` / `update()`:

- `FieldValue.serverTimestamp()` — replaced with the server's current timestamp.
- `FieldValue.increment(n)` — adds `n` to the existing numeric field (missing field is treated as 0). Result is `int` only when both operands are `int`; any `double` operand yields a `double`.
- `FieldValue.arrayUnion(...values)` — appends elements not already present (deep equality).
- `FieldValue.arrayRemove(...values)` — removes all occurrences of the given values.
- `FieldValue.delete()` — supported via the standard `updateMask` removal path.
- Numeric `maximum` / `minimum` transforms (used internally by some SDKs) are also applied.

**Note on transactions (Level 1 semantics):** `db.runTransaction()` is supported with atomic commits but **no conflict detection**. All writes inside a transaction are applied atomically (single-threaded in-memory operations). If the callback throws, no writes persist. Conflicts between concurrent transactions are NOT detected — the emulator does not track read sets or document versions, and the SDK's `ABORTED` retry path is never triggered. This is sufficient for the vast majority of single-threaded test suites. Tests that require real Firestore isolation semantics must run against production.

When an unsupported RPC is called, the emulator logs a clear warning to stderr (or throws if `logs.onUnimplemented` is `'throw'`). See **Configuration** for `onUnimplemented`.

### Firebase Auth (HTTP)

The Auth emulator exposes the Identity Toolkit REST API under `/identitytoolkit.googleapis.com/v1/projects/:projectId/...`. The Firebase Admin SDK uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set. The server stores users in memory; use the returned `AuthServer.getStorage()` for test helpers.

| API endpoint | Supported | Used by Firebase Admin SDK for |
|--------------|-----------|---------------------------------|
| `accounts:lookup` | Yes | `getUser(uid)`, `getUserByEmail(email)`, `getUserByPhoneNumber(phone)` |
| `accounts` (POST) | Yes | `createUser({ email, password, ... })` |
| `accounts:delete` | Yes | `deleteUser(uid)` |
| `accounts:update` | Yes | `updateUser(uid, { ... })` |
| `accounts:sendOobCode` | Yes | `generatePasswordResetLink(email)` (returns URL with extractable `oobCode`) |
| `accounts:signInWithCustomToken` | Yes | Exchanges a custom token (from `auth.createCustomToken(uid)`) for an `idToken` that `verifyIdToken()` accepts. Auto-creates the user if missing. |
| Other Identity Toolkit endpoints | No | Return 404 (e.g. email/password sign-in, phone sign-in, IDP, email link, etc.) |

### Firebase Storage (HTTP)

The Storage emulator implements the Google Cloud Storage JSON API. The Firebase Admin SDK (via `@google-cloud/storage`) uses it when `FIREBASE_STORAGE_EMULATOR_HOST` is set. Files are stored in memory as Buffers; use `storageServer.getStorage()` for test helpers.

| Operation | Supported | Used by Firebase Admin SDK for |
|-----------|-----------|--------------------------------|
| Resumable upload (POST + PUT) | Yes | `file.save()`, `file.createWriteStream()` |
| Multipart upload (`resumable: false`) | Yes | `file.save(data, { resumable: false })` |
| Download (`alt=media`) | Yes | `file.download()` |
| Get metadata | Yes | `file.getMetadata()` |
| Update metadata | Yes | `file.setMetadata()` |
| Delete object | Yes | `file.delete()` |
| List objects | Yes | `bucket.getFiles()` (with `prefix`, `delimiter`, pagination) |
| Firebase download URL (`/v0/...`) | Yes | `getDownloadURL()` |
| Signed URL (`file.getSignedUrl(...)`) | Yes (via monkey-patch) | `getSignedUrl({action, expires})` returns a URL pointing to the emulator |
| Copy / rewrite | No | — |
| Compose objects | No | — |

**Note on signed URLs**: The official `@google-cloud/storage` `getSignedUrl()` requires real service account credentials to sign URLs cryptographically. To support it in emulator mode, `startStorageServer()` monkey-patches `File.prototype.getSignedUrl` to return a URL pointing to the emulator (no signature). `stopStorageServer()` restores the original method.

### Firebase Remote Config (HTTP)

The Remote Config emulator implements the Firebase Remote Config REST API. The Firebase Admin SDK uses it when `FIREBASE_REMOTE_CONFIG_URL_BASE` is set.

| Endpoint | Supported | Used by Firebase Admin SDK for |
|----------|-----------|--------------------------------|
| `GET /v1/projects/{projectId}/remoteConfig` | Yes | `remoteConfig().getTemplate()` |
| `PUT /v1/projects/{projectId}/remoteConfig` | Yes | `remoteConfig().publishTemplate(template)` with `If-Match` etag check |
| `POST /v1/projects/{projectId}/remoteConfig:rollback` | No | Returns 501 |
| `GET /v1/projects/{projectId}/remoteConfig:listVersions` | No | Returns 501 |
| `GET /v1/projects/{projectId}/namespaces/firebase-server/serverRemoteConfig` | No | Returns 501 |
| `PUT /v1/projects/{projectId}/remoteConfig?validate_only=true` | No | Returns 501 |

ETags are tracked as `etag-{projectId}-{counter}` (counter increments on each successful publish). `If-Match: *` is accepted to force-publish without an etag check. The emulator does not evaluate conditions or maintain version history.

## Technical notes

- **IPv6 (Firestore):** The Firestore server binds to `[::]:port` so it accepts both IPv4 and IPv6 (Firebase Admin SDK may use IPv6).
- **Proto source:** The server loads definitions from `proto/v1.json` (bundled in this package). That file is a copy of `@google-cloud/firestore/build/protos/v1.json`. To update it (e.g. after upgrading firebase-admin), run: `cp node_modules/@google-cloud/firestore/build/protos/v1.json proto/v1.json` from the firebase-mocker package root.

## License

MIT
