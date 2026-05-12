import { expect } from 'chai';
import { RemoteConfigStorage } from '../../src/firebase-remote-config/storage';

describe('RemoteConfigStorage', () => {
  it('returns empty initial template when no initialTemplate provided', () => {
    const s = new RemoteConfigStorage('demo-project');
    const t = s.getTemplate();
    expect(t.parameters).to.deep.equal({});
    expect(t.conditions).to.deep.equal([]);
    expect(t.parameterGroups).to.deep.equal({});
    expect(t.etag).to.equal('etag-demo-project-0');
    expect(t.version.versionNumber).to.equal('0');
    expect(t.version.updateOrigin).to.equal('ADMIN_SDK_NODE');
    expect(t.version.updateType).to.equal('INCREMENTAL_UPDATE');
    expect(typeof t.version.updateTime).to.equal('string');
  });

  it('populates initial template fields when provided', () => {
    const s = new RemoteConfigStorage('demo-project', {
      parameters: {
        foo: { defaultValue: { value: 'bar' }, valueType: 'STRING' },
      },
    });
    const t = s.getTemplate();
    expect(t.parameters).to.deep.equal({
      foo: { defaultValue: { value: 'bar' }, valueType: 'STRING' },
    });
    expect(t.etag).to.equal('etag-demo-project-1');
    expect(t.version.versionNumber).to.equal('1');
  });

  it('accepts updateTemplate when ifMatch matches current etag', () => {
    const s = new RemoteConfigStorage('demo-project');
    const result = s.updateTemplate(
      {
        parameters: {
          a: { defaultValue: { value: '1' }, valueType: 'STRING' },
        },
      },
      'etag-demo-project-0',
    );
    expect(result.ok).to.equal(true);
    if (!result.ok) {
      return;
    }
    expect(result.template.etag).to.equal('etag-demo-project-1');
    expect(result.template.version.versionNumber).to.equal('1');
    expect(result.template.parameters.a.defaultValue.value).to.equal('1');
  });

  it('accepts updateTemplate when ifMatch is "*"', () => {
    const s = new RemoteConfigStorage('demo-project');
    const result = s.updateTemplate({ parameters: {} }, '*');
    expect(result.ok).to.equal(true);
    if (!result.ok) {
      return;
    }
    expect(result.template.etag).to.equal('etag-demo-project-1');
  });

  it('rejects updateTemplate with stale etag and returns current', () => {
    const s = new RemoteConfigStorage('demo-project');
    s.updateTemplate({ parameters: {} }, '*');
    const result = s.updateTemplate({ parameters: {} }, 'etag-demo-project-0');
    expect(result.ok).to.equal(false);
    if (result.ok) {
      return;
    }
    expect(result.currentEtag).to.equal('etag-demo-project-1');
  });

  it('clear() resets state', () => {
    const s = new RemoteConfigStorage('demo-project');
    s.updateTemplate(
      {
        parameters: {
          x: { defaultValue: { value: 'y' }, valueType: 'STRING' },
        },
      },
      '*',
    );
    s.clear();
    const t = s.getTemplate();
    expect(t.parameters).to.deep.equal({});
    expect(t.etag).to.equal('etag-demo-project-0');
    expect(t.version.versionNumber).to.equal('0');
  });

  it('increments etag counter on each successful publish', () => {
    const s = new RemoteConfigStorage('demo-project');
    let r = s.updateTemplate({}, 'etag-demo-project-0');
    expect(r.ok).to.equal(true);
    r = s.updateTemplate({}, 'etag-demo-project-1');
    expect(r.ok).to.equal(true);
    r = s.updateTemplate({}, 'etag-demo-project-2');
    expect(r.ok).to.equal(true);
    expect(s.getTemplate().etag).to.equal('etag-demo-project-3');
    expect(s.getTemplate().version.versionNumber).to.equal('3');
  });
});
