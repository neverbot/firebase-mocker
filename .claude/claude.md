# Firebase Mocker

## Overview

`firebase-mocker` is a TypeScript package that runs **local in-process emulators of Firebase services**. It is designed to be launched from code (e.g. from a backend's test setup) and exposes one or more local servers on `localhost`. Once started, it sets the standard Firebase emulator environment variables so that an unmodified Firebase Admin SDK in the same process talks to these local servers instead of real Firebase.

The goal is **fast, hermetic testing**: no network, no real Firebase project, no credentials — but the application code under test keeps using the regular `firebase-admin` APIs.

Currently emulated:

- **Firestore** — gRPC server (default port `3333`), sets `FIRESTORE_EMULATOR_HOST`.
- **Firebase Auth** — HTTP server implementing the Identity Toolkit REST API (default port `9099`), sets `FIREBASE_AUTH_EMULATOR_HOST`.

Each server can be started independently or both together. State is kept in memory; nothing is persisted to disk.

## Public API (entry point)

The package exposes a `firebaseMocker` object from `src/index.ts`:

- `firebaseMocker.startFirestoreServer({ port, host, projectId })` — start the Firestore gRPC emulator. Sets `FIRESTORE_EMULATOR_HOST` before returning.
- `firebaseMocker.stopFirestoreServer()` — stop it.
- `firebaseMocker.startAuthServer({ port, host, projectId })` — start the Auth HTTP emulator. Sets `FIREBASE_AUTH_EMULATOR_HOST` before returning.
- `firebaseMocker.stopAuthServer()` — stop it.
- `addConfig({ logs: { verboseGrpcLogs, verboseAuthLogs, onUnimplemented } })` — configure logging and the policy for unimplemented RPCs (`'warn'` default, or `'throw'` for strict CI).

Both `startFirestoreServer` / `startAuthServer` must be called **before** `admin.initializeApp(...)` so the env vars are picked up by the Admin SDK.

## Repo layout

- `src/` — TypeScript sources
  - `src/index.ts` — public API (`firebaseMocker`)
  - `src/firestore/` — Firestore gRPC server, handlers, in-memory storage, query/value conversion
  - `src/firebase-auth/` — Firebase Auth HTTP server (Identity Toolkit endpoints) and in-memory user store
  - `src/config.ts`, `src/logger.ts`, `src/types.ts` — shared config, logging, types
- `proto/v1.json` — Firestore proto definitions, copied from `@google-cloud/firestore/build/protos/v1.json`
- `test/` — Mocha + Chai test suites (`firestore.spec.ts`, `firestore-server.spec.ts`, `firestore-utils.spec.ts`, `firebase-auth.spec.ts`, plus `firestore/` and `firebase-auth/` helpers and `_setup.ts`)
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
- `RunQuery` (with `where` field/composite/unary filters, `orderBy`, `limit`, `offset`)
- `RunAggregationQuery` (COUNT supported; sum/avg currently return 0)
- `CreateDocument`
- `UpdateDocument`
- `DeleteDocument`
- `Commit` (used by `set()`, `add()`, `update()`, `delete()`)
- `BatchGetDocuments` (used by `doc.get()`)
- `Listen` (real-time listeners, streaming)
- `Write` (write stream used by client SDK)
- `ListCollectionIds` (`doc.ref.listCollections()`)

### Not implemented (return UNIMPLEMENTED)

- `BatchWrite`
- `BeginTransaction`
- `Rollback`
- Cursor-based pagination inside `RunQuery`: `startAt` / `startAfter` / `endAt` / `endBefore` are ignored. Offset+limit pagination works.

When an unimplemented RPC is called, the server logs a clear warning to stderr (or throws if `logs.onUnimplemented === 'throw'`).

## Firebase Auth emulator (HTTP)

The Auth server implements a subset of the Identity Toolkit REST API under `/identitytoolkit.googleapis.com/v1/projects/:projectId/...`, in memory. The Firebase Admin Auth client uses it when `FIREBASE_AUTH_EMULATOR_HOST` is set.

Implemented endpoints:

- `accounts:lookup` — `getUser(uid)`, `getUserByEmail(email)`, `getUserByPhoneNumber(phone)`
- `accounts` (POST) — `createUser({ email, password, ... })`
- `accounts:delete` — `deleteUser(uid)`
- `accounts:update` — `updateUser(uid, { ... })`

Other Identity Toolkit endpoints (custom token sign-in, email link, etc.) return 404.

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

- Transactions (`BeginTransaction`, `Rollback`) and `BatchWrite`
- Cursor-based pagination (`startAt` / `endAt`) in `RunQuery`
- Optional persistence to disk
- Security rules emulation
- Additional Identity Toolkit endpoints (custom tokens, email links, etc.)
