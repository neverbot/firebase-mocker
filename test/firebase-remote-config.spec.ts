/**
 * E2E tests for Firebase Remote Config emulator using the firebase-admin SDK.
 */

import { expect } from 'chai';
import * as admin from 'firebase-admin';
import { firebaseMocker } from '../src';

const PORT = 19296;
const PROJECT_ID = 'demo-project';

// Custom credential that returns a fake access token without making any network
// calls. The Remote Config emulator ignores the bearer token entirely, but
// firebase-admin's AuthorizedHttpClient requires a credential object.
const fakeCredential: admin.credential.Credential = {
  async getAccessToken(): Promise<admin.GoogleOAuthAccessToken> {
    return Promise.resolve({
      access_token: 'fake-access-token-for-emulator',
      expires_in: 3600,
    });
  },
};

describe('Remote Config E2E (firebase-admin)', () => {
  let app: admin.app.App;

  beforeEach(async () => {
    await firebaseMocker.startRemoteConfigServer({
      port: PORT,
      host: 'localhost',
      projectId: PROJECT_ID,
    });
    app = admin.initializeApp(
      {
        projectId: PROJECT_ID,
        credential: fakeCredential,
      },
      `rc-test-${Date.now()}`,
    );
  });

  afterEach(async () => {
    await app.delete();
    await firebaseMocker.stopRemoteConfigServer();
  });

  it('getTemplate() returns empty template with non-empty etag', async () => {
    const t = await app.remoteConfig().getTemplate();
    expect(t.parameters).to.deep.equal({});
    expect(t.conditions).to.deep.equal([]);
    expect(t.parameterGroups).to.deep.equal({});
    expect(t.etag).to.equal('etag-demo-project-0');
  });

  it('publishTemplate() updates parameters and returns new template', async () => {
    const t = await app.remoteConfig().getTemplate();
    t.parameters.welcome_msg = {
      defaultValue: { value: 'Hello world' },
      valueType: 'STRING',
    };
    const updated = await app.remoteConfig().publishTemplate(t);
    expect(updated.etag).to.equal('etag-demo-project-1');
    expect(updated.parameters.welcome_msg.defaultValue).to.deep.equal({
      value: 'Hello world',
    });
  });

  it('publishTemplate() with stale etag throws etag-mismatch error', async () => {
    const t = await app.remoteConfig().getTemplate();
    await app.remoteConfig().publishTemplate(t);
    let caught: Error | undefined;
    try {
      await app.remoteConfig().publishTemplate(t);
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
    const t = await app.remoteConfig().getTemplate();
    t.parameters.feature_x = {
      defaultValue: { value: 'true' },
      valueType: 'BOOLEAN',
    };
    await app.remoteConfig().publishTemplate(t);
    const after = await app.remoteConfig().getTemplate();
    expect(after.parameters.feature_x.defaultValue).to.deep.equal({
      value: 'true',
    });
    expect(after.etag).to.equal('etag-demo-project-1');
  });

  it('full cycle: get -> modify -> publish -> get -> see changes', async () => {
    const t1 = await app.remoteConfig().getTemplate();
    t1.parameters.a = { defaultValue: { value: '1' }, valueType: 'STRING' };
    await app.remoteConfig().publishTemplate(t1);

    const t2 = await app.remoteConfig().getTemplate();
    t2.parameters.b = { defaultValue: { value: '2' }, valueType: 'STRING' };
    await app.remoteConfig().publishTemplate(t2);

    const final = await app.remoteConfig().getTemplate();
    expect(final.parameters.a.defaultValue).to.deep.equal({ value: '1' });
    expect(final.parameters.b.defaultValue).to.deep.equal({ value: '2' });
    expect(final.version.versionNumber).to.equal('2');
  });

  it('respects initialTemplate when provided', async () => {
    await firebaseMocker.stopRemoteConfigServer();
    await firebaseMocker.startRemoteConfigServer({
      port: PORT,
      host: 'localhost',
      projectId: PROJECT_ID,
      initialTemplate: {
        parameters: {
          preset: {
            defaultValue: { value: 'preset-val' },
            valueType: 'STRING',
          },
        },
      },
    });
    const t = await app.remoteConfig().getTemplate();
    expect(t.parameters.preset.defaultValue).to.deep.equal({
      value: 'preset-val',
    });
    expect(t.etag).to.equal('etag-demo-project-1');
  });
});
