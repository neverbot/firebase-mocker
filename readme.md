# Firebase Mocker

A modern TypeScript-based emulator that provides **two separate servers**: one for **Firestore** (gRPC) and one for **Firebase Auth** (HTTP). The Firebase Admin SDK connects to these local servers when the corresponding emulator environment variables are set.

## Overview

Firebase Mocker can run:

1. **Firestore emulator** — A **gRPC server** that implements the Firestore service contract. The Admin SDK talks to it when `FIRESTORE_EMULATOR_HOST` is set. Use it for local development, integration tests, and CI without real Firestore credentials.

2. **Firebase Auth emulator** — An **HTTP server** that implements the Identity Toolkit REST API. The Admin SDK Auth API (e.g. `getUserByEmail`, `createUser`, `deleteUser`) uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set. Use it to test auth flows without hitting production Firebase Auth.

You can start **one or both** servers in the same process (e.g. in your test setup). Each server is independent: start only what your tests or app need.

## Installation

### As a dependency in another project (local development)

#### Option 1: npm link (recommended)

```bash
# In firebase-mocker package
cd firebase-mocker
npm run build
npm link

# In your project
cd /path/to/your/project
npm link firebase-mocker
```

#### Option 2: file: protocol

In your project's `package.json`:

```json
{
  "devDependencies": {
    "firebase-mocker": "file:../path/to/firebase-mocker/firebase-mocker"
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

Configuration is passed when starting each server. You can use a single config object for both, or pass different options to `startFirestoreServer()` and `startAuthServer()`.

### Firestore server options

When calling `startFirestoreServer(config)`:

- **port** — gRPC server port (default `3333`)
- **host** — Bind address (e.g. `'localhost'`, or `'0.0.0.0'` for all interfaces)
- **projectId** — Project ID (must match the one used in your Firebase Admin app)
- **logs.verboseGrpcLogs** — Set to `true` to enable verbose gRPC request/response logs

### Firebase Auth server options

When calling `startAuthServer(config)`:

- **projectId** — Project ID (optional; can come from config)
- **auth.port** — HTTP server port for the Auth emulator (default `9099`)
- **auth.host** — Bind address (default `'localhost'`)

Example config object used by both:

```typescript
const config = {
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
  logs: { verboseGrpcLogs: false },
  auth: {
    port: 9099,
    host: 'localhost',
  },
};
```

## Usage

Start **only the servers you need**. Each method sets the corresponding environment variable so the Firebase Admin SDK uses that emulator.

### 1. Firestore server only

Start the Firestore gRPC emulator. The Admin SDK will use it for `admin.firestore()` when `FIRESTORE_EMULATOR_HOST` is set (done automatically by `startFirestoreServer()`).

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

// Start the Firestore emulator (sets FIRESTORE_EMULATOR_HOST)
const firestoreServer = await firebaseMocker.startFirestoreServer({
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
});

// Initialize Firebase Admin — it will use the emulator
admin.initializeApp({ projectId: 'my-project' });
const db = admin.firestore();

// Use Firestore as usual; all calls go to the emulator via gRPC
const ref = db.collection('users').doc('user1');
await ref.set({ name: 'Jane', email: 'jane@example.com' });
const snap = await ref.get();
console.log(snap.data());

// When done (e.g. after tests)
await firestoreServer.stop();
```

- **Before** initializing the Admin SDK, call `startFirestoreServer()` so `FIRESTORE_EMULATOR_HOST` is set.
- Use the returned `FirestoreServer` for `getStorage()` (test helpers) or `stop()` to shut down.

### 2. Firebase Auth server only

Start the Auth HTTP emulator. The Admin SDK will use it for `admin.auth()` when `FIREBASE_AUTH_EMULATOR_HOST` is set (done automatically by `startAuthServer()`).

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

// Start the Auth emulator (sets FIREBASE_AUTH_EMULATOR_HOST)
const authServer = await firebaseMocker.startAuthServer({
  projectId: 'my-project',
  auth: { port: 9099, host: 'localhost' },
});

// Initialize Firebase Admin — Auth API will use the emulator
admin.initializeApp({ projectId: 'my-project' });
const auth = admin.auth();

// Use Auth as usual (createUser, getUserByEmail, deleteUser, etc.)
const user = await auth.createUser({ email: 'jane@example.com', password: 'secret' });
console.log(user.uid);

// When done
await firebaseMocker.stopAuthServer();
```

- **Before** initializing the Admin SDK, call `startAuthServer()` so `FIREBASE_AUTH_EMULATOR_HOST` is set.
- Use `authServer.getStorage()` to access the in-memory user store (e.g. to assert created users in tests).
- Use `firebaseMocker.stopAuthServer()` to stop the **last** started Auth server (the package keeps a reference to it).

### 3. Both servers (Firestore + Auth)

You can run both emulators in the same process (e.g. in test setup). Start both **before** initializing the Admin SDK.

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

const config = {
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
  auth: { port: 9099, host: 'localhost' },
};

// Start both emulators; both env vars are set
const firestoreServer = await firebaseMocker.startFirestoreServer(config);
const authServer = await firebaseMocker.startAuthServer(config);

// Now both admin.firestore() and admin.auth() use the emulators
admin.initializeApp({ projectId: 'my-project' });
const db = admin.firestore();
const auth = admin.auth();

// ... use db and auth ...

// Teardown: stop Firestore explicitly; Auth via helper
await firestoreServer.stop();
await firebaseMocker.stopAuthServer();
```

### Connecting from an application — summary

1. Start the emulator(s) **before** calling `admin.initializeApp()`.
2. `startFirestoreServer()` sets `FIRESTORE_EMULATOR_HOST`; `startAuthServer()` sets `FIREBASE_AUTH_EMULATOR_HOST`.
3. Use the same `projectId` in the config and in `initializeApp({ projectId })`.

### Integration tests

Use `startFirestoreServer()` and/or `startAuthServer()` so the real Firebase Admin SDK talks to the emulators. Use the returned server instances: `getStorage()` for test data helpers, `stop()` (Firestore) or `stopAuthServer()` (Auth) for teardown.

## Implemented APIs

### Firestore (gRPC)

The Firestore emulator implements these gRPC methods:

| Method | Used by Firebase Admin SDK for |
|--------|---------------------------------|
| `GetDocument` | Single document fetch |
| `ListDocuments` | List documents in a collection |
| `RunQuery` | Queries (e.g. `collection.get()`, `where()`) |
| `CreateDocument` | Create document with server-generated ID |
| `UpdateDocument` | Update existing document |
| `DeleteDocument` | Delete document |
| `Commit` | Writes: `set()`, `add()`, `update()`, `delete()` |
| `BatchGetDocuments` | Batched reads, e.g. `doc().get()` |

### Firebase Auth (HTTP)

The Auth emulator exposes the Identity Toolkit REST API under `/identitytoolkit.googleapis.com/v1/projects/:projectId/...`. The Firebase Admin SDK uses it for operations such as `createUser`, `getUserByEmail`, `deleteUser`, and `getUser` when `FIREBASE_AUTH_EMULATOR_HOST` is set. The server stores users in memory; use the returned `AuthServer.getStorage()` for test helpers.

## Technical notes

- **Firestore protocol:** gRPC only. The Firestore server uses the same proto definitions as the official client (`@google-cloud/firestore`). No REST API for Firestore.
- **Auth protocol:** HTTP (REST). The Auth server implements the Identity Toolkit API that the Firebase Admin Auth client calls when `FIREBASE_AUTH_EMULATOR_HOST` is set.
- **IPv6 (Firestore):** The Firestore server binds to `[::]:port` so it accepts both IPv4 and IPv6 (Firebase Admin SDK may use IPv6).
- **Proto source:** The server loads definitions from `proto/v1.json` (bundled in this package). That file is a copy of `@google-cloud/firestore/build/protos/v1.json`. To update it (e.g. after upgrading firebase-admin), run: `cp node_modules/@google-cloud/firestore/build/protos/v1.json proto/v1.json` from the firebase-mocker package root.
- **Implementation alignment:** When adding or changing RPCs, compare with the definitions in `@google-cloud/firestore/build/protos/`.
- **Field naming:** Response messages use a mix of `snake_case` and `camelCase` depending on the RPC (CommitResponse/WriteResult use snake_case; BatchGetDocumentsResponse/Document use camelCase). Do not change them without testing both document creation and reads.
- **Null values:** In gRPC `Value` messages, null must be encoded as `nullValue: 0`, not `nullValue: null`, or the Firebase Admin SDK may throw when decoding.

## Status

Current focus is on correctness and compatibility with the Firebase Admin SDK for:

- Basic CRUD (get, set, update, delete)
- Queries with filters (e.g. `where`)
- Batch get (e.g. `doc(id).get()`)

Possible future work:

- Transactions
- Real-time listeners
- Persistence to disk
- Security rules emulation

## License

MIT
