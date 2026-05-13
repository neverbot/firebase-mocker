import { expect } from 'chai';
import { config } from '../../src/config';
import { RemoteConfigServer } from '../../src/firebase-remote-config/server';

const PORT = 19297;

describe('Remote Config unimplemented endpoints', () => {
  let server: RemoteConfigServer;

  beforeEach(async () => {
    config.addConfig({ logs: { onUnimplemented: 'warn' } });
    server = new RemoteConfigServer({
      port: PORT,
      host: 'localhost',
      projectId: 'demo-project',
    });
    await server.start();
  });

  afterEach(async () => {
    await server.stop();
    config.addConfig({ logs: { onUnimplemented: 'warn' } });
  });

  it('returns 501 for rollback', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig:rollback`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      },
    );
    expect(res.status).to.equal(501);
    const body = (await res.json()) as { error: { status: string } };
    expect(body.error.status).to.equal('UNIMPLEMENTED');
  });

  it('returns 501 for listVersions', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig:listVersions`,
    );
    expect(res.status).to.equal(501);
  });

  it('returns 501 for namespaces/firebase-server/serverRemoteConfig', async () => {
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/namespaces/firebase-server/serverRemoteConfig`,
    );
    expect(res.status).to.equal(501);
  });

  it('throws (server-side) when onUnimplemented=throw and unimplemented endpoint is hit', async () => {
    config.addConfig({ logs: { onUnimplemented: 'throw' } });
    const res = await fetch(
      `http://localhost:${PORT}/v1/projects/demo-project/remoteConfig:rollback`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      },
    );
    // Express error handler catches the throw → 500
    expect(res.status).to.equal(500);
  });
});
