/**
 * Unit tests for FirestoreServer (src/firestore/server.ts).
 * Exercises the server's public API so coverage is attributed to server.ts.
 *
 * Coverage of server.ts remains around ~56% and is difficult to increase without
 * modifying the source. Most of the uncovered code lives inside gRPC callbacks
 * (requestDeserialize, responseSerialize, responseDeserialize) registered in
 * createServiceDefinition(). Those closures run at runtime on every request
 * (get, set, commit, etc.), but coverage tools (c8/Istanbul) often do not
 * attribute execution inside such callbacks to server.ts when they are
 * invoked by the gRPC library. The rest of the uncovered code includes
 * private helpers (e.g. extractArrayValueFromMessage, manualDecodeArrayValue)
 * and error paths (e.g. start() reject when bind fails). Raising coverage
 * would require refactoring server.ts (e.g. extracting callback bodies into
 * named methods), which we avoid to keep the implementation unchanged.
 */

import { expect } from 'chai';
import { FirestoreServer } from '../src/firestore';
import type { FirestoreDocument } from '../src/types';
import { getFirestoreServer } from './_setup';

describe('FirestoreServer (unit)', () => {
  let server: ReturnType<typeof getFirestoreServer>;

  before(function () {
    server = getFirestoreServer();
  });

  describe('getConfig', () => {
    it('returns config with projectId and port', () => {
      const config = server.getConfig();
      expect(config).to.be.an('object');
      expect(config.projectId).to.equal('test-project');
      expect(config.port).to.equal(3333);
    });
  });

  describe('getStorage', () => {
    it('returns storage instance', () => {
      const storage = server.getStorage();
      expect(storage).to.exist;
      expect(typeof storage.getDocument).to.equal('function');
      expect(typeof storage.setDocument).to.equal('function');
      expect(typeof storage.listDocuments).to.equal('function');
    });
  });

  describe('debugLog', () => {
    it('does not throw', () => {
      expect(() => server.debugLog()).to.not.throw();
    });
  });

  describe('parseDocumentPath', () => {
    it('parses valid document path', () => {
      const path = 'projects/my-proj/databases/(default)/documents/users/user1';
      const parsed = server.parseDocumentPath(path);
      expect(parsed).to.deep.equal({
        projectId: 'my-proj',
        databaseId: '(default)',
        collectionId: 'users',
        docId: 'user1',
      });
    });
    it('returns null for invalid path', () => {
      expect(server.parseDocumentPath('invalid')).to.be.null;
    });
  });

  describe('inferFieldType', () => {
    it('returns arrayValue for list-like names', () => {
      expect(server.inferFieldType('items')).to.equal('arrayValue');
    });
    it('returns mapValue for data-like names', () => {
      expect(server.inferFieldType('metadata')).to.equal('mapValue');
    });
  });

  describe('detectFieldType', () => {
    it('detects type from value', () => {
      expect(server.detectFieldType({ stringValue: 'x' })).to.equal(
        'stringValue',
      );
      expect(server.detectFieldType({ integerValue: '1' })).to.equal(
        'integerValue',
      );
      expect(server.detectFieldType({ nullValue: null })).to.equal('nullValue');
    });
    it('returns null for unknown', () => {
      expect(server.detectFieldType({} as any)).to.be.null;
    });
  });

  describe('reconstructDocumentFields', () => {
    it('returns fields when no fieldTypes', () => {
      const doc: FirestoreDocument = {
        name: 'projects/p/databases/db/documents/c/d',
        fields: { x: { stringValue: 'a' } },
      };
      expect(server.reconstructDocumentFields(doc)).to.deep.equal({
        x: { stringValue: 'a' },
      });
    });
    it('reconstructs missing field when fieldTypes present', () => {
      const doc: FirestoreDocument = {
        name: 'p',
        fields: {},
        fieldTypes: { missing: 'stringValue' },
      };
      const out = server.reconstructDocumentFields(doc);
      expect(out.missing).to.deep.equal({ stringValue: '' });
    });
  });

  describe('applyOrderBy', () => {
    it('sorts documents by field', () => {
      const docs: FirestoreDocument[] = [
        {
          name: 'p/c/d/b',
          fields: { score: { integerValue: '30' } },
        },
        {
          name: 'p/c/d/a',
          fields: { score: { integerValue: '10' } },
        },
      ];
      const sorted = server.applyOrderBy(docs, [
        { field: { fieldPath: 'score' }, direction: 1 },
      ]);
      expect(sorted[0].fields?.score?.integerValue).to.equal('10');
      expect(sorted[1].fields?.score?.integerValue).to.equal('30');
    });
    it('returns empty array unchanged', () => {
      expect(
        server.applyOrderBy([], [{ field: { fieldPath: 'x' } }]),
      ).to.deep.equal([]);
    });
  });

  describe('applyQueryFilters', () => {
    it('filters documents by field EQUAL', () => {
      const docs: FirestoreDocument[] = [
        {
          name: 'p/c/d/a',
          fields: { status: { stringValue: 'active' } },
        },
        {
          name: 'p/c/d/b',
          fields: { status: { stringValue: 'inactive' } },
        },
      ];
      const filtered = server.applyQueryFilters(docs, {
        fieldFilter: {
          field: { fieldPath: 'status' },
          op: 'EQUAL',
          value: { stringValue: 'active' },
        },
      });
      expect(filtered).to.have.length(1);
      expect(filtered[0].name).to.equal('p/c/d/a');
    });
    it('returns docs unchanged when no filter', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { x: { stringValue: '1' } } },
      ];
      expect(server.applyQueryFilters(docs, null as any)).to.have.length(1);
    });
  });
});
