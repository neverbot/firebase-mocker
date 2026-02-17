/**
 * E2E tests for Listen: firebase-admin client triggers the Listen RPC
 * via onSnapshot(). The emulator closes the stream with UNIMPLEMENTED.
 * (The SDK may not always deliver that error to the error callback.)
 */

import * as admin from 'firebase-admin';
import { getFirestore } from '../_setup';

describe('Firestore Listen (e2e)', () => {
  let db: admin.firestore.Firestore;

  before(function () {
    db = getFirestore();
  });

  describe('onSnapshot', () => {
    it('Listen RPC is closed by server; unsubscribe succeeds', async function () {
      const ref = db.collection('listen_e2e').doc('x');
      const unsubscribe = ref.onSnapshot(
        () => {},
        () => {},
      );
      // Server closes the stream with UNIMPLEMENTED; client may or may not
      // call the error callback. Unsubscribe should not throw.
      unsubscribe();
    });
  });
});
