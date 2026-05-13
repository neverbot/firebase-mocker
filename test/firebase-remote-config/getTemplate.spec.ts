import { expect } from 'chai';
import { RemoteConfigServer } from '../../src/firebase-remote-config/server';

const PORT = 19299;

describe('Remote Config GET /v1/projects/:projectId/remoteConfig', () => {
  let server: RemoteConfigServer;

  beforeEach(async () => {
    server = new RemoteConfigServer({
      port: PORT,
      host: 'localhost',
      projectId: 'demo-project',
    });
    await server.start();
  });

  afterEach(async () => {
    await server.stop();
  });

  it('returns 200 with empty template and ETag header', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
    );
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal('etag-demo-project-0');
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
