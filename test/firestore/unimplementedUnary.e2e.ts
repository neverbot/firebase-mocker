/**
 * E2E tests for unimplemented unary RPCs.
 *
 * These tests do not cover real feature code. They verify that when the client
 * uses Firestore features that are not yet supported by the emulator, the
 * server correctly responds with UNIMPLEMENTED (and optionally that a warning
 * is emitted). This documents the behaviour for batch writes.
 */
