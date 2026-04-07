# Firebase Mocker

A TypeScript-based emulator of the Firebase services. It provides **separate servers**. The Firebase Admin SDK connects to these local servers when the corresponding emulator environment variables are set.

 - **Firestore** (gRPC)
 - **Firebase Auth** (HTTP)

## Overview

Firebase Mocker can run:

1. **Firestore emulator** — A **gRPC server** that implements the Firestore service contract. The Admin SDK talks to it when `FIRESTORE_EMULATOR_HOST` is set. Use it for local development, integration tests, and CI without real Firestore credentials.

2. **Firebase Auth emulator** — An **HTTP server** that implements the Identity Toolkit REST API. The Admin SDK Auth API (e.g. `getUserByEmail`, `createUser`, `deleteUser`) uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set. Use it to test auth flows without hitting production Firebase Auth.

You can start **one or both** servers in the same process (e.g. in your test setup). Each server is independent: start only what your tests or app need.

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

Configuration is passed when starting each server. You can pass different options to `startFirestoreServer()` and `startAuthServer()`.

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

### Common options

For logs, use `addConfig({ logs: { ... } })` **before** starting servers:

- **verboseGrpcLogs** — Log every gRPC call (default `false`). Set `true` for debugging.
- **verboseAuthLogs** — Log Auth API requests (default `false`).
- **onUnimplemented** — When an RPC is not implemented: `'warn'` (default) writes a clear message to stderr and returns UNIMPLEMENTED; `'throw'` throws so the process fails. Example: `addConfig({ logs: { onUnimplemented: 'throw' } })` for strict CI.

## Usage

Start only the servers you need (one or both) **before** initializing the Firebase Admin SDK. Each `start*` call sets the matching emulator environment variable so the Admin SDK transparently routes to the local server.

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

// Start the emulators you need (omit either call to skip that server)
const firestoreServer = await firebaseMocker.startFirestoreServer({
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
});
const authServer = await firebaseMocker.startAuthServer({
  port: 9099,
  host: 'localhost',
  projectId: 'my-project',
});

// Initialize Firebase Admin — it will pick up the emulator env vars
admin.initializeApp({ projectId: 'my-project' });
const db = admin.firestore();
const auth = admin.auth();

// Use the SDKs as usual; all calls go to the local emulators
await db.collection('users').doc('user1').set({ name: 'Jane' });
const user = await auth.createUser({ email: 'jane@example.com', password: 'secret' });

// Teardown (e.g. after tests)
await firebaseMocker.stopFirestoreServer();
await firebaseMocker.stopAuthServer();
```

Notes:

- Call `startFirestoreServer()` / `startAuthServer()` **before** `admin.initializeApp(...)` so `FIRESTORE_EMULATOR_HOST` / `FIREBASE_AUTH_EMULATOR_HOST` are set in time.
- Each server is independent — call only the one(s) you need; `stop*` only the ones you started.
- Use `firestoreServer.getStorage()` / `authServer.getStorage()` to inspect the in-memory state from tests.

## Implemented APIs

### Firestore (gRPC)

The Firestore emulator implements these gRPC methods:

| Method | Supported | Used by Firebase Admin SDK for |
|--------|-----------|---------------------------------|
| `GetDocument` | Yes | Single document fetch (`doc(id).get()`) |
| `ListDocuments` | Yes | List documents in a collection |
| `RunQuery` | Yes | Queries (`collection.get()`, `where()` field/composite/unary, `orderBy()` ASC/DESC including `__name__`, `limit()`, `offset()`, and cursor pagination `startAt()` / `startAfter()` / `endAt()` / `endBefore()`) |
| `RunAggregationQuery` | Yes | Aggregation queries, e.g. `count().get()` (COUNT supported; sum/avg return 0) |
| `CreateDocument` | Yes | Create document with server-generated ID |
| `UpdateDocument` | Yes | Update existing document |
| `DeleteDocument` | Yes | Delete document |
| `Commit` | Yes | Writes: `set()`, `add()`, `update()`, `delete()` |
| `BatchGetDocuments` | Yes | Batched reads, e.g. `doc(id).get()` |
| `Listen` | Yes | Real-time listeners (streaming) |
| `Write` | Yes | Write stream (used by client SDK) |
| `ListCollectionIds` | Yes | List subcollection IDs under a document (`doc.ref.listCollections()`) |
| `BatchWrite` | No | Returns UNIMPLEMENTED; see `onUnimplemented` in Configuration |
| `BeginTransaction` | No | Returns UNIMPLEMENTED |
| `Rollback` | No | Returns UNIMPLEMENTED |

When an unsupported RPC is called, the emulator logs a clear warning to stderr (or throws if `logs.onUnimplemented` is `'throw'`). See **Configuration** for `onUnimplemented`.

### Firebase Auth (HTTP)

The Auth emulator exposes the Identity Toolkit REST API under `/identitytoolkit.googleapis.com/v1/projects/:projectId/...`. The Firebase Admin SDK uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set. The server stores users in memory; use the returned `AuthServer.getStorage()` for test helpers.

| API endpoint | Supported | Used by Firebase Admin SDK for |
|--------------|-----------|---------------------------------|
| `accounts:lookup` | Yes | `getUser(uid)`, `getUserByEmail(email)`, `getUserByPhoneNumber(phone)` |
| `accounts` (POST) | Yes | `createUser({ email, password, ... })` |
| `accounts:delete` | Yes | `deleteUser(uid)` |
| `accounts:update` | Yes | `updateUser(uid, { ... })` |
| Other Identity Toolkit endpoints | No | Return 404 (e.g. custom token sign-in, email link, etc.) |

## Technical notes

- **Firestore protocol:** gRPC only. The Firestore server uses the same proto definitions as the official client (`@google-cloud/firestore`). No REST API for Firestore.
- **Auth protocol:** HTTP (REST). The Auth server implements the Identity Toolkit API that the Firebase Admin Auth client calls when `FIREBASE_AUTH_EMULATOR_HOST` is set.
- **IPv6 (Firestore):** The Firestore server binds to `[::]:port` so it accepts both IPv4 and IPv6 (Firebase Admin SDK may use IPv6).
- **Proto source:** The server loads definitions from `proto/v1.json` (bundled in this package). That file is a copy of `@google-cloud/firestore/build/protos/v1.json`. To update it (e.g. after upgrading firebase-admin), run: `cp node_modules/@google-cloud/firestore/build/protos/v1.json proto/v1.json` from the firebase-mocker package root.
- **Implementation alignment:** When adding or changing RPCs, compare with the definitions in `@google-cloud/firestore/build/protos/`.
- **Field naming:** Response messages use a mix of `snake_case` and `camelCase` depending on the RPC (CommitResponse/WriteResult use snake_case; BatchGetDocumentsResponse/Document use camelCase). Do not change them without testing both document creation and reads.
- **Null values:** In gRPC `Value` messages, null must be encoded as `nullValue: 0`, not `nullValue: null`, or the Firebase Admin SDK may throw when decoding.

## Status

**Firestore emulator** — Implemented and in use:

- Basic CRUD (get, set, update, delete) via Commit, GetDocument, BatchGetDocuments, CreateDocument, UpdateDocument, DeleteDocument
- Queries with filters, orderBy, limit, offset (RunQuery); composite and unary filters supported
- Batch get (`doc(id).get()`)
- Real-time listeners (Listen) and Write stream
- Aggregation queries (RunAggregationQuery; COUNT supported)
- ListCollectionIds (`doc.ref.listCollections()`)

**Firebase Auth emulator** — Implemented: lookup, createUser, deleteUser, updateUser (Identity Toolkit API in memory).

**Possible future work:**

- Transactions (BeginTransaction, Rollback), BatchWrite
- Persistence to disk
- Security rules emulation

## License

MIT
