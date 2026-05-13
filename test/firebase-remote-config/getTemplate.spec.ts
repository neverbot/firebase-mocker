import { expect } from 'chai';
import { getRemoteConfigStorage } from '../_setup';

const PORT = 9299;
const PROJECT_ID = 'test-project';

describe('Remote Config GET /v1/projects/:projectId/remoteConfig', () => {
  beforeEach(function () {
    getRemoteConfigStorage().clear();
  });

  it('returns 200 with empty template and ETag header', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/${PROJECT_ID}/remoteConfig`,
    );
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal(`etag-${PROJECT_ID}-0`);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.parameters).to.deep.equal({});
    expect(body.conditions).to.deep.equal([]);
    expect(body.parameterGroups).to.deep.equal({});
    expect((body.version as { versionNumber: string }).versionNumber).to.equal(
      '0',
    );
    expect('etag' in body).to.equal(false);
  });

  it('returns 404 when projectId in path does not match configured projectId', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/wrong-project/remoteConfig`,
    );
    expect(res.status).to.equal(404);
  });
});
