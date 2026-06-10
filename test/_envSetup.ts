/**
 * Test-side register hook. Wraps `src/register.ts` (the same module that
 * consumers import as `firebase-mocker/register`) so our own test suite
 * exercises the public entry point.
 */

import '../src/register';
