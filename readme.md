# Firebase Mocker

A modern TypeScript-based Firestore emulator server for local development and testing.

## Overview

Firebase Mocker provides a local HTTP server that emulates the Firestore REST API, allowing applications to connect to it as if it were the real Google Firestore service. This is useful for:

- Local development without Firebase credentials
- Integration testing
- Offline development
- CI/CD pipelines

## Installation

### As a dependency in another project (local development)

To use `firebase-mocker` in another project without publishing it to npm, you have two options:

#### Option 1: npm link (Recommended)

```bash
# In firebase-mocker
cd firebase-mocker
npm run build
npm link

# In your test project
cd /path/to/your/project
npm link firebase-mocker
```

#### Option 2: file: protocol

In your test project's `package.json`:

```json
{
  "dependencies": {
    "firebase-mocker": "file:../firebase-mocker/firebase-mocker"
  }
}
```

Then run `npm install`.

See [local_install.md](./local_install.md) for more details.

### Development setup

```bash
npm install
```

## Development

```bash
# Install dependencies
npm install

# Run in development mode (with hot reload)
npm run dev

# Build TypeScript
npm run build

# Run production build
npm start
```

## Configuration

The server can be configured via environment variables in a `.env` file or directly as environment variables.

### Using .env file (Recommended)

Create a `.env` file in the project root:

```env
# Server host (default: localhost)
HOST=localhost

# Server port (default: 3333)
PORT=3333

# Default project ID (default: demo-project)
PROJECT_ID=demo-project
```

You can copy `.env.example` as a template:

```bash
cp .env.example .env
```

### Using environment variables directly

```bash
PORT=9090 HOST=0.0.0.0 PROJECT_ID=my-project npm run dev
```

### Configuration priority

1. Configuration passed programmatically (highest priority)
2. Environment variables from `.env` file
3. System environment variables
4. Default values (lowest priority)

## Usage

### Starting the Server

```bash
npm run dev
```

The server will start on `http://localhost:3333` by default.

### Starting the Server Programmatically

You can start the server programmatically from your code:

```typescript
import { firebaseMocker } from 'firebase-mocker';

// Create Firebase mocks
const mockFirestore = firebaseMocker.MockFirestore();
const mockAuth = firebaseMocker.MockAuthentication();
const mockStorage = firebaseMocker.MockStorage();

// Start the Firestore server (optional, for REST API access)
await mockFirestore.start();

// Use the mocks in your tests...

// Stop the server when done
await mockFirestore.stop();
```

Or with custom configuration:

```typescript
import { firebaseMocker } from 'firebase-mocker';

const mockFirestore = firebaseMocker.MockFirestore({
  port: 9090,
  host: '0.0.0.0',
  projectId: 'my-test-project',
});

await mockFirestore.start();
```

### Connecting from an Application

To connect to the emulator from a Firebase Admin SDK application, you need to configure it to use the emulator endpoint:

```typescript
import * as admin from 'firebase-admin';
import { firebaseMocker } from 'firebase-mocker';

// Create and start the emulator server
const mockFirestore = firebaseMocker.MockFirestore({
  port: 3333,
  projectId: 'demo-project',
});

await mockFirestore.start();

// Initialize Firebase Admin with emulator settings
admin.initializeApp({
  projectId: 'demo-project',
});

// Configure Firestore to use the emulator
process.env.FIRESTORE_EMULATOR_HOST = 'localhost:3333';

const db = admin.firestore();

// Now you can use Firestore as normal
const docRef = db.collection('users').doc('user1');
await docRef.set({ name: 'John Doe', email: 'john@example.com' });

const doc = await docRef.get();
console.log(doc.data());

// Stop the server when done
await mockFirestore.stop();
```

### REST API Endpoints

The server implements the following Firestore REST API endpoints:

- `GET /v1/projects/{projectId}/databases/{databaseId}/documents/{collectionId}/{docId}` - Get a document
- `GET /v1/projects/{projectId}/databases/{databaseId}/documents/{collectionId}` - List documents
- `POST /v1/projects/{projectId}/databases/{databaseId}/documents/{collectionId}` - Create a document
- `PATCH /v1/projects/{projectId}/databases/{databaseId}/documents/{collectionId}/{docId}` - Update a document
- `DELETE /v1/projects/{projectId}/databases/{databaseId}/documents/{collectionId}/{docId}` - Delete a document

### Example: Using curl

```bash
# Create a document
curl -X POST http://localhost:3333/v1/projects/demo-project/databases/\(default\)/documents/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'

# Get a document
curl http://localhost:3333/v1/projects/demo-project/databases/\(default\)/documents/users/{docId}

# List documents
curl http://localhost:3333/v1/projects/demo-project/databases/\(default\)/documents/users
```

## Status

This is an early version focusing on basic CRUD operations. Future versions will include:

- Query support (where, orderBy, limit, etc.)
- Transactions
- Batch writes
- Real-time listeners (via WebSockets or Server-Sent Events)
- Persistence to disk
- Security rules emulation

## License

MIT
