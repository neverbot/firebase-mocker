import { expect } from 'chai';
import { getRemoteConfigStorage } from '../_setup';

const PORT = 9299;
const PROJECT_ID = 'test-project';
const URL = `http://localhost:${PORT}/v1/projects/${PROJECT_ID}/remoteConfig`;
const WRONG_URL = `http://localhost:${PORT}/v1/projects/wrong-project/remoteConfig`;

describe('Remote Config PUT /v1/projects/:projectId/remoteConfig', () => {
  beforeEach(function () {
    getRemoteConfigStorage().clear();
  });

  it('returns 200 and increments etag when If-Match matches', async () => {
    const res = await fetch(URL, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'If-Match': `etag-${PROJECT_ID}-0`,
      },
      body: JSON.stringify({
        parameters: {
          foo: { defaultValue: { value: 'bar' }, valueType: 'STRING' },
        },
      }),
    });
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal(`etag-${PROJECT_ID}-1`);
    const body = (await res.json()) as Record<string, unknown>;
    expect(
      (body.parameters as Record<string, { defaultValue: { value: string } }>)
        .foo.defaultValue.value,
    ).to.equal('bar');
  });

  it('returns 200 and force-updates when If-Match is "*"', async () => {
    const res = await fetch(URL, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
      body: JSON.stringify({ parameters: {} }),
    });
    expect(res.status).to.equal(200);
    expect(res.headers.get('etag')).to.equal(`etag-${PROJECT_ID}-1`);
  });

  it('returns 412 when If-Match is stale', async () => {
    await fetch(URL, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
      body: JSON.stringify({ parameters: {} }),
    });
    const res = await fetch(URL, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'If-Match': `etag-${PROJECT_ID}-0`,
      },
      body: JSON.stringify({ parameters: {} }),
    });
    expect(res.status).to.equal(412);
    const body = (await res.json()) as { error: { status: string } };
    expect(body.error.status).to.equal('FAILED_PRECONDITION');
  });

  it('returns 400 when If-Match header is missing', async () => {
    const res = await fetch(URL, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ parameters: {} }),
    });
    expect(res.status).to.equal(400);
  });

  it('returns 501 when validate_only=true', async () => {
    const res = await fetch(`${URL}?validate_only=true`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
      body: JSON.stringify({ parameters: {} }),
    });
    expect(res.status).to.equal(501);
  });

  it('returns 404 when projectId does not match', async () => {
    const res = await fetch(WRONG_URL, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'If-Match': '*' },
      body: JSON.stringify({ parameters: {} }),
    });
    expect(res.status).to.equal(404);
  });
});
