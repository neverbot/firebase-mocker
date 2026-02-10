# Firebase Mocker

A modern TypeScript-based Firestore emulator that speaks the Firestore gRPC API. It allows the Firebase Admin SDK to connect to a local server as if it were Google Firestore.

## Overview

Firebase Mocker runs a **gRPC server** that implements the Firestore service contract. The Firebase Admin SDK uses gRPC to talk to Firestore, so this emulator plugs in by setting `FIRESTORE_EMULATOR_HOST`. Use it for:

- Local development without Firebase credentials
- Integration and E2E tests
- Offline development
- CI/CD pipelines

**Important:** The server uses **gRPC** only. There are no REST endpoints; the Firebase Admin SDK connects over gRPC using the same protocol as production Firestore.

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

See [local_install.md](./local_install.md) for more details.

### Development setup

```bash
npm install
npm run build
```

## Configuration

Configuration is passed as a parameter when initializing the emulator:

```typescript
import { firebaseMocker } from 'firebase-mocker';

console.log('Starting firebase-mocker server...');
const firebaseMockerConfig = {
  port: 3333,
  host: 'localhost',
  projectId: 'avance-dev',
  logs: {
    verboseGrpcLogs: false,
  },
};
const firebaseMockerServer =
  await firebaseMocker.startFirestoreServer(firebaseMockerConfig);
```

Options:

- **port** — gRPC server port (e.g. `3333`)
- **host** — Bind address (e.g. `'localhost'`, or `'0.0.0.0'` for all interfaces)
- **projectId** — Firestore project ID (must match the one used in your Firebase Admin app)
- **logs.verboseGrpcLogs** — Set to `true` to enable verbose gRPC request/response logs

## Usage

### Starting the gRPC server

**From the command line:**

```bash
npm run dev
# or
npm start
```

The gRPC server listens on the configured host/port (e.g. `localhost:3333`). There is no HTTP URL; the Firebase Admin SDK connects via gRPC.

**From your test or app code:**

```typescript
import { firebaseMocker } from 'firebase-mocker';
import * as admin from 'firebase-admin';

// Start the Firestore gRPC emulator (sets FIRESTORE_EMULATOR_HOST automatically)
const server = await firebaseMocker.startFirestoreServer({
  port: 3333,
  host: 'localhost',
  projectId: 'my-project',
});

// FIRESTORE_EMULATOR_HOST is already set by startFirestoreServer
// Initialize Firebase Admin so it uses the emulator
admin.initializeApp({ projectId: 'my-project' });
const db = admin.firestore();

// Use Firestore as usual — all calls go to the emulator via gRPC
const ref = db.collection('users').doc('user1');
await ref.set({ name: 'Jane', email: 'jane@example.com' });
const snap = await ref.get();
console.log(snap.data());

// When done (e.g. after tests)
await server.stop();
```

### Connecting from an application

1. Start the emulator **before** initializing the Firebase Admin SDK.
2. `startFirestoreServer()` sets `process.env.FIRESTORE_EMULATOR_HOST` so the Admin SDK targets your emulator.
3. Initialize the app with the same `projectId` you passed to the emulator.

Example:

```typescript
import * as admin from 'firebase-admin';
import { firebaseMocker } from 'firebase-mocker';

// 1. Start emulator first
await firebaseMocker.startFirestoreServer({
  port: 3333,
  host: 'localhost',
  projectId: 'demo-project',
});

// 2. Then initialize Firebase Admin (it will use FIRESTORE_EMULATOR_HOST)
admin.initializeApp({ projectId: 'demo-project' });
const db = admin.firestore();

// 3. Use Firestore normally
const docRef = db.collection('events').doc('event1');
await docRef.set({ name: 'My Event', start: new Date() });
const doc = await docRef.get();
console.log(doc.exists, doc.data());
```

### Mock Firestore and Mock Authentication

For in-process mocking without a real server (e.g. unit tests with no network), you can use:

```typescript
import { firebaseMocker } from 'firebase-mocker';

const mockFirestore = firebaseMocker.MockFirestore();
const mockAuth = firebaseMocker.MockAuthentication();

// Use mocks in your tests...
```

For integration tests that use the real Firebase Admin SDK, prefer `startFirestoreServer()` so the SDK talks to the gRPC emulator.

## Implemented gRPC methods

The emulator implements the following Firestore gRPC methods:

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

## Development

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Run (starts gRPC server)
npm start

# Development with watch (if configured)
npm run dev

# Tests
npm run test
npm run test:only
```

## Technical notes

- **Protocol:** gRPC only. The server uses the same Firestore proto definitions as the official client (`@google-cloud/firestore`). No REST API.
- **IPv6:** The server binds to `[::]:port` so it accepts both IPv4 and IPv6 (Firebase Admin SDK may use IPv6).
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
