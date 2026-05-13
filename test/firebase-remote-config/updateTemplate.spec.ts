import { expect } from 'chai';
import { RemoteConfigServer } from '../../src/firebase-remote-config/server';

const PORT = 19298;

describe('Remote Config PUT /v1/projects/:projectId/remoteConfig', () => {
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

  it('returns 200 and increments etag when If-Match matches', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
      {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'If-Match': 'etag-demo-project-0',
        },
        body: JSON.stringify({
          parameters: {
            foo: { defaultValue: { value: 'bar' }, valueType: 'STRING' },
          },
        }),
      },
    );
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal('etag-demo-project-1');
    const body = (await res.json()) as Record<string, unknown>;
    expect(
      (body.parameters as Record<string, { defaultValue: { value: string } }>)
        .foo.defaultValue.value,
    ).to.equal('bar');
  });

  it('returns 200 and force-updates when If-Match is "*"', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal('etag-demo-project-1');
  });

  it('returns 412 when If-Match is stale', async () => {
    await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
      {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'If-Match': 'etag-demo-project-0',
        },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    expect(res.status).to.equal(412);
    const body = (await res.json()) as { error: { status: string } };
    expect(body.error.status).to.equal('FAILED_PRECONDITION');
  });

  it('returns 400 when If-Match header is missing', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    expect(res.status).to.equal(400);
  });

  it('returns 501 when validate_only=true', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig?validate_only=true`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    expect(res.status).to.equal(501);
  });

  it('returns 404 when projectId does not match', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/wrong-project/remoteConfig`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
        body: JSON.stringify({ parameters: {} }),
      },
    );
    expect(res.status).to.equal(404);
  });
});
