/**
 * E2E tests for Firebase Remote Config emulator using the firebase-admin SDK.
 *
 * The shared Remote Config server is started by `_setup.ts`; this file just
 * creates its own admin app (Remote Config requires an explicit credential
 * because it routes through `AuthorizedHttpClient`) and clears storage between
 * tests.
 */

import { expect } from 'chai';
import {
  App,
  Credential,
  deleteApp,
  GoogleOAuthAccessToken,
  initializeApp,
} from 'firebase-admin/app';
import { getRemoteConfig } from 'firebase-admin/remote-config';
import { getRemoteConfigStorage } from './_setup';

const PROJECT_ID = 'test-project';

// Custom credential that returns a fake access token without making any network
// calls. The Remote Config emulator ignores the bearer token entirely, but
// firebase-admin's AuthorizedHttpClient requires a credential object.
const fakeCredential: Credential = {
  async getAccessToken(): Promise<GoogleOAuthAccessToken> {
    return Promise.resolve({
      access_token: 'fake-access-token-for-emulator',
      expires_in: 3600,
    });
  },
};

describe('Remote Config E2E (firebase-admin)', () => {
  let app: App;

  before(function () {
    app = initializeApp(
      { projectId: PROJECT_ID, credential: fakeCredential },
      `rc-test-${Date.now()}`,
    );
  });

  after(async function () {
    await deleteApp(app);
  });

  beforeEach(function () {
    getRemoteConfigStorage().clear();
  });

  it('getTemplate() returns empty template with non-empty etag', async () => {
    const t = await getRemoteConfig(app).getTemplate();
    expect(t.parameters).to.deep.equal({});
    expect(t.conditions).to.deep.equal([]);
    expect(t.parameterGroups).to.deep.equal({});
    expect(t.etag).to.equal(`etag-${PROJECT_ID}-0`);
  });

  it('publishTemplate() updates parameters and returns new template', async () => {
    const t = await getRemoteConfig(app).getTemplate();
    t.parameters.welcome_msg = {
      defaultValue: { value: 'Hello world' },
      valueType: 'STRING',
    };
    const updated = await getRemoteConfig(app).publishTemplate(t);
    expect(updated.etag).to.equal(`etag-${PROJECT_ID}-1`);
    expect(updated.parameters.welcome_msg.defaultValue).to.deep.equal({
      value: 'Hello world',
    });
  });

  it('publishTemplate() with stale etag throws etag-mismatch error', async () => {
    const t = await getRemoteConfig(app).getTemplate();
    await getRemoteConfig(app).publishTemplate(t);
    let caught: Error | undefined;
    try {
      await getRemoteConfig(app).publishTemplate(t);
    } catch (err) {
      caught = err as Error;
    }
    expect(caught).to.exist;
    // The firebase-admin SDK maps HTTP 412 FAILED_PRECONDITION to 'failed-precondition'
    expect((caught as { code?: string }).code).to.contain(
      'failed-precondition',
    );
  });

  it('getTemplate() after publish reflects changes', async () => {
    const t = await getRemoteConfig(app).getTemplate();
    t.parameters.feature_x = {
      defaultValue: { value: 'true' },
      valueType: 'BOOLEAN',
    };
    await getRemoteConfig(app).publishTemplate(t);
    const after = await getRemoteConfig(app).getTemplate();
    expect(after.parameters.feature_x.defaultValue).to.deep.equal({
      value: 'true',
    });
    expect(after.etag).to.equal(`etag-${PROJECT_ID}-1`);
  });

  it('full cycle: get -> modify -> publish -> get -> see changes', async () => {
    const t1 = await getRemoteConfig(app).getTemplate();
    t1.parameters.a = { defaultValue: { value: '1' }, valueType: 'STRING' };
    await getRemoteConfig(app).publishTemplate(t1);

    const t2 = await getRemoteConfig(app).getTemplate();
    t2.parameters.b = { defaultValue: { value: '2' }, valueType: 'STRING' };
    await getRemoteConfig(app).publishTemplate(t2);

    const final = await getRemoteConfig(app).getTemplate();
    expect(final.parameters.a.defaultValue).to.deep.equal({ value: '1' });
    expect(final.parameters.b.defaultValue).to.deep.equal({ value: '2' });
    expect(final.version.versionNumber).to.equal('2');
  });
});
