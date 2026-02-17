/**
 * Unit tests for src/firestore/utils.ts only.
 * These tests exercise the pure utility functions (paths, value conversion,
 * query filters, orderBy, etc.) in isolation.
 */

import { expect } from 'chai';
import {
  GRPC_NULL_VALUE,
  toTimestamp,
  toFirestoreValue,
  fromFirestoreValue,
  toFirestoreDocument,
  fromFirestoreDocument,
  buildDocumentPath,
  parseDocumentPath,
  toGrpcValue,
  sanitizeGrpcValueForResponse,
  sanitizeGrpcFieldsForResponse,
  normalizeGrpcValueToFirestoreValue,
  reconstructDocumentFields,
  timestampToMs,
  getFieldValueByPath,
  compareValuesForOrder,
  safeNormalizeFilterValue,
  valuesEqual,
  valueLessThan,
  valueGreaterThan,
  arrayContains,
  valueIn,
  arrayContainsAny,
  compareFieldValueWithNormalized,
  applyFieldFilter,
  applyUnaryFilter,
  applyCompositeFilter,
  applyQueryFilters,
  applyOrderBy,
  inferFieldType,
  detectFieldType,
  generateDocumentId,
  toGrpcFields,
} from '../src/firestore/utils';
import type { FirestoreDocument } from '../src/types';

describe('firestore/utils', () => {
  describe('GRPC_NULL_VALUE', () => {
    it('is 0', () => {
      expect(GRPC_NULL_VALUE).to.equal(0);
    });
  });

  describe('toTimestamp', () => {
    it('converts Date to seconds and nanos', () => {
      const d = new Date('2021-06-15T12:00:00.000Z');
      const t = toTimestamp(d);
      expect(t.seconds).to.equal(1623758400);
      expect(t.nanos).to.equal(0);
    });
    it('handles sub-second precision', () => {
      const d = new Date('2021-06-15T12:00:00.500Z');
      const t = toTimestamp(d);
      expect(t.seconds).to.equal(1623758400);
      expect(t.nanos).to.equal(500000000);
    });
  });

  describe('toFirestoreValue', () => {
    it('converts null/undefined to nullValue', () => {
      expect(toFirestoreValue(null)).to.deep.equal({ nullValue: null });
      expect(toFirestoreValue(undefined)).to.deep.equal({ nullValue: null });
    });
    it('converts boolean, number, string', () => {
      expect(toFirestoreValue(true)).to.deep.equal({ booleanValue: true });
      expect(toFirestoreValue(42)).to.deep.equal({ integerValue: '42' });
      expect(toFirestoreValue(3.14)).to.deep.equal({ doubleValue: 3.14 });
      expect(toFirestoreValue('hello')).to.deep.equal({ stringValue: 'hello' });
    });
    it('converts Date to timestampValue', () => {
      const d = new Date('2021-01-01T00:00:00.000Z');
      expect(toFirestoreValue(d)).to.deep.equal({
        timestampValue: '2021-01-01T00:00:00.000Z',
      });
    });
    it('converts array and object', () => {
      expect(toFirestoreValue([1, 2])).to.deep.equal({
        arrayValue: {
          values: [{ integerValue: '1' }, { integerValue: '2' }],
        },
      });
      expect(toFirestoreValue({ a: 1 })).to.deep.equal({
        mapValue: {
          fields: { a: { integerValue: '1' } },
        },
      });
    });
    it('converts unknown to nullValue', () => {
      expect(toFirestoreValue(Symbol('x'))).to.deep.equal({ nullValue: null });
    });
  });

  describe('fromFirestoreValue', () => {
    it('converts nullValue to null', () => {
      expect(fromFirestoreValue({ nullValue: null })).to.be.null;
    });
    it('converts primitive types', () => {
      expect(fromFirestoreValue({ booleanValue: true })).to.equal(true);
      expect(fromFirestoreValue({ integerValue: '10' })).to.equal(10);
      expect(fromFirestoreValue({ doubleValue: 1.5 })).to.equal(1.5);
      expect(fromFirestoreValue({ stringValue: 'x' })).to.equal('x');
    });
    it('converts timestampValue to Date', () => {
      const v = fromFirestoreValue({
        timestampValue: '2021-01-01T00:00:00.000Z',
      });
      expect(v).to.be.instanceOf(Date);
      expect((v as Date).toISOString()).to.equal('2021-01-01T00:00:00.000Z');
    });
    it('converts arrayValue and mapValue', () => {
      expect(
        fromFirestoreValue({
          arrayValue: { values: [{ stringValue: 'a' }, { stringValue: 'b' }] },
        }),
      ).to.deep.equal(['a', 'b']);
      expect(
        fromFirestoreValue({
          mapValue: { fields: { k: { stringValue: 'v' } } },
        }),
      ).to.deep.equal({ k: 'v' });
    });
    it('returns null for invalid input', () => {
      expect(fromFirestoreValue(null as any)).to.be.null;
      expect(fromFirestoreValue({} as any)).to.be.null;
    });
  });

  describe('toFirestoreDocument / fromFirestoreDocument', () => {
    it('round-trips document', () => {
      const path = 'projects/p/databases/db/documents/coll/doc1';
      const data = { name: 'Alice', count: 10 };
      const doc = toFirestoreDocument(path, data);
      expect(doc.name).to.equal(path);
      expect(doc.fields.name).to.deep.equal({ stringValue: 'Alice' });
      expect(doc.fields.count).to.deep.equal({ integerValue: '10' });
      expect(fromFirestoreDocument(doc)).to.deep.equal(data);
    });
    it('fromFirestoreDocument with empty fields', () => {
      expect(
        fromFirestoreDocument({
          name: 'projects/p/databases/db/documents/c/d',
          fields: {},
        }),
      ).to.deep.equal({});
    });
  });

  describe('buildDocumentPath / parseDocumentPath', () => {
    it('builds and parses root collection path', () => {
      const path = buildDocumentPath('proj', 'db', 'users', 'u1');
      expect(path).to.equal('projects/proj/databases/db/documents/users/u1');
      const parsed = parseDocumentPath(path);
      expect(parsed).to.deep.equal({
        projectId: 'proj',
        databaseId: 'db',
        collectionId: 'users',
        docId: 'u1',
      });
    });
    it('builds and parses subcollection path', () => {
      const path = 'projects/p/databases/db/documents/users/u1/posts/p1';
      const parsed = parseDocumentPath(path);
      expect(parsed).to.deep.equal({
        projectId: 'p',
        databaseId: 'db',
        collectionId: 'users/u1/posts',
        docId: 'p1',
      });
    });
    it('parseDocumentPath returns null for invalid path', () => {
      expect(parseDocumentPath('invalid')).to.be.null;
      expect(parseDocumentPath('projects/p/databases/db/documents')).to.be.null;
    });
  });

  describe('toGrpcValue', () => {
    it('returns nullValue for null/invalid', () => {
      expect(toGrpcValue(null as any)).to.deep.equal({
        nullValue: GRPC_NULL_VALUE,
      });
      expect(toGrpcValue({})).to.deep.equal({ nullValue: GRPC_NULL_VALUE });
    });
    it('converts string and number', () => {
      expect(toGrpcValue({ stringValue: 'x' })).to.deep.equal({
        stringValue: 'x',
      });
      expect(toGrpcValue({ integerValue: '5' })).to.deep.equal({
        integerValue: '5',
      });
    });
    it('converts arrayValue recursively', () => {
      expect(
        toGrpcValue({
          arrayValue: { values: [{ stringValue: 'a' }] },
        }),
      ).to.deep.equal({
        arrayValue: { values: [{ stringValue: 'a' }] },
      });
    });
  });

  describe('sanitizeGrpcValueForResponse', () => {
    it('converts null/undefined to nullValue 0', () => {
      expect(sanitizeGrpcValueForResponse(null)).to.deep.equal({
        nullValue: GRPC_NULL_VALUE,
      });
    });
    it('recursively sanitizes arrayValue and mapValue', () => {
      expect(
        sanitizeGrpcValueForResponse({
          arrayValue: { values: [{ stringValue: 'x' }] },
        }),
      ).to.deep.equal({
        arrayValue: { values: [{ stringValue: 'x' }] },
      });
      expect(
        sanitizeGrpcValueForResponse({
          mapValue: { fields: { a: { stringValue: 'y' } } },
        }),
      ).to.deep.equal({
        mapValue: { fields: { a: { stringValue: 'y' } } },
      });
    });
  });

  describe('sanitizeGrpcFieldsForResponse', () => {
    it('sanitizes each field', () => {
      const out = sanitizeGrpcFieldsForResponse({
        name: { stringValue: 'Test' },
      });
      expect(out).to.deep.equal({ name: { stringValue: 'Test' } });
    });
  });

  describe('normalizeGrpcValueToFirestoreValue', () => {
    it('handles null_value and snake_case', () => {
      expect(
        normalizeGrpcValueToFirestoreValue({ null_value: 0 }),
      ).to.deep.equal({ nullValue: null });
      expect(
        normalizeGrpcValueToFirestoreValue({ string_value: 'hi' }),
      ).to.deep.equal({ stringValue: 'hi' });
    });
    it('handles timestamp_value object', () => {
      const v = normalizeGrpcValueToFirestoreValue({
        timestamp_value: { seconds: 1609459200, nanos: 0 },
      });
      expect(v).to.have.property('timestampValue');
      expect((v as any).timestampValue).to.include('2021-01-01');
    });
    it('handles depth limit', () => {
      let deep: any = { arrayValue: { values: [] } };
      for (let i = 0; i < 51; i++) {
        deep = { arrayValue: { values: [deep] } };
      }
      const result = normalizeGrpcValueToFirestoreValue(deep);
      let current: any = result;
      for (let i = 0; i < 52; i++) {
        if (
          current?.arrayValue?.values?.[0] !== undefined &&
          current?.arrayValue?.values?.[0] !== null
        ) {
          current = current.arrayValue.values[0];
        } else {
          break;
        }
      }
      expect(current).to.deep.equal({ nullValue: null });
    });
  });

  describe('reconstructDocumentFields', () => {
    it('returns fields when no fieldTypes', () => {
      const doc: FirestoreDocument = {
        name: 'projects/p/db/documents/c/d',
        fields: { x: { stringValue: 'a' } },
      };
      expect(reconstructDocumentFields(doc)).to.deep.equal({
        x: { stringValue: 'a' },
      });
    });
    it('reconstructs missing field with fieldTypes', () => {
      const doc: FirestoreDocument = {
        name: 'p',
        fields: {},
        fieldTypes: { missing: 'stringValue' },
      };
      expect(reconstructDocumentFields(doc).missing).to.deep.equal({
        stringValue: '',
      });
    });
  });

  describe('timestampToMs', () => {
    it('parses string timestamp', () => {
      expect(
        timestampToMs({ timestampValue: '2021-01-01T00:00:00.000Z' }),
      ).to.equal(1609459200000);
    });
    it('parses object seconds/nanos', () => {
      expect(
        timestampToMs({
          timestampValue: { seconds: 1609459200, nanos: 0 },
        } as any),
      ).to.equal(1609459200000);
    });
    it('returns null when no timestamp', () => {
      expect(timestampToMs({ stringValue: 'x' })).to.be.null;
    });
  });

  describe('getFieldValueByPath', () => {
    it('returns top-level field', () => {
      const fields = { name: { stringValue: 'x' } };
      expect(getFieldValueByPath(fields, 'name')).to.deep.equal({
        stringValue: 'x',
      });
    });
    it('returns nested field via dot path', () => {
      const fields = {
        addr: {
          mapValue: {
            fields: { city: { stringValue: 'NY' } },
          },
        },
      };
      expect(getFieldValueByPath(fields, 'addr.city')).to.deep.equal({
        stringValue: 'NY',
      });
    });
    it('returns undefined for empty path or missing field', () => {
      expect(getFieldValueByPath({}, '')).to.be.undefined;
      expect(getFieldValueByPath({ a: { stringValue: 'x' } }, 'b')).to.be
        .undefined;
    });
  });

  describe('compareValuesForOrder', () => {
    it('compares nulls', () => {
      expect(compareValuesForOrder(null, null)).to.equal(0);
      expect(compareValuesForOrder(null, 'x')).to.equal(1);
      expect(compareValuesForOrder('x', null)).to.equal(-1);
    });
    it('compares strings and numbers', () => {
      expect(
        compareValuesForOrder({ stringValue: 'a' }, { stringValue: 'b' }),
      ).to.be.lessThan(0);
      expect(
        compareValuesForOrder({ integerValue: '10' }, { integerValue: '20' }),
      ).to.be.lessThan(0);
    });
  });

  describe('valuesEqual', () => {
    it('compares equal primitives', () => {
      expect(valuesEqual({ stringValue: 'a' }, { stringValue: 'a' })).to.be
        .true;
      expect(valuesEqual({ integerValue: '1' }, { integerValue: '1' })).to.be
        .true;
    });
    it('returns false for different values', () => {
      expect(valuesEqual({ stringValue: 'a' }, { stringValue: 'b' })).to.be
        .false;
    });
  });

  describe('valueLessThan / valueGreaterThan', () => {
    it('compares strings and numbers', () => {
      expect(valueLessThan({ stringValue: 'a' }, { stringValue: 'b' })).to.be
        .true;
      expect(valueGreaterThan({ integerValue: '20' }, { integerValue: '10' }))
        .to.be.true;
    });
  });

  describe('arrayContains / valueIn / arrayContainsAny', () => {
    it('arrayContains finds value in array', () => {
      expect(
        arrayContains(
          { arrayValue: { values: [{ stringValue: 'x' }] } },
          { stringValue: 'x' },
        ),
      ).to.be.true;
      expect(
        arrayContains(
          { arrayValue: { values: [{ stringValue: 'x' }] } },
          { stringValue: 'y' },
        ),
      ).to.be.false;
    });
    it('valueIn checks if field in compare array', () => {
      expect(
        valueIn(
          { stringValue: 'a' },
          {
            arrayValue: {
              values: [{ stringValue: 'a' }, { stringValue: 'b' }],
            },
          },
        ),
      ).to.be.true;
    });
    it('arrayContainsAny', () => {
      expect(
        arrayContainsAny(
          { arrayValue: { values: [{ stringValue: 'a' }] } },
          { arrayValue: { values: [{ stringValue: 'a' }] } },
        ),
      ).to.be.true;
    });
  });

  describe('compareFieldValueWithNormalized', () => {
    it('EQUAL and NOT_EQUAL', () => {
      expect(
        compareFieldValueWithNormalized({ stringValue: 'x' }, 'EQUAL', {
          stringValue: 'x',
        }),
      ).to.be.true;
      expect(
        compareFieldValueWithNormalized(undefined, 'NOT_EQUAL', {
          stringValue: 'x',
        }),
      ).to.be.true;
    });
    it('LESS_THAN and GREATER_THAN', () => {
      expect(
        compareFieldValueWithNormalized({ integerValue: '1' }, '<', {
          integerValue: '2',
        }),
      ).to.be.true;
      expect(
        compareFieldValueWithNormalized({ integerValue: '3' }, '>', {
          integerValue: '2',
        }),
      ).to.be.true;
    });
  });

  describe('applyFieldFilter', () => {
    const logger = undefined;
    const reconstruct = (doc: FirestoreDocument) => doc.fields ?? {};

    it('filters by EQUAL', () => {
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
      const filtered = applyFieldFilter(
        docs,
        {
          field: { fieldPath: 'status' },
          op: 'EQUAL',
          value: { stringValue: 'active' },
        },
        logger,
        reconstruct,
      );
      expect(filtered).to.have.length(1);
      expect(filtered[0].name).to.equal('p/c/d/a');
    });

    it('returns all docs when field/op/value missing', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { x: { stringValue: '1' } } },
      ];
      expect(applyFieldFilter(docs, {}, logger, reconstruct)).to.have.length(1);
    });
  });

  describe('applyUnaryFilter', () => {
    const reconstruct = (doc: FirestoreDocument) => doc.fields ?? {};

    it('IS_NULL matches missing field', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: {} },
        { name: 'p/c/d/b', fields: { x: { stringValue: '1' } } },
      ];
      const filtered = applyUnaryFilter(
        docs,
        { op: 'IS_NULL', field: { fieldPath: 'x' } },
        reconstruct,
      );
      expect(filtered).to.have.length(1);
      expect(filtered[0].name).to.equal('p/c/d/a');
    });

    it('IS_NOT_NULL matches present field', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { x: { stringValue: '1' } } },
      ];
      const filtered = applyUnaryFilter(
        docs,
        { op: 'IS_NOT_NULL', field: { fieldPath: 'x' } },
        reconstruct,
      );
      expect(filtered).to.have.length(1);
    });
  });

  describe('applyCompositeFilter', () => {
    const logger = undefined;
    it('AND applies each filter', () => {
      const docs: FirestoreDocument[] = [
        {
          name: 'p/c/d/a',
          fields: { a: { stringValue: '1' }, b: { stringValue: '2' } },
        },
        {
          name: 'p/c/d/b',
          fields: { a: { stringValue: '1' }, b: { stringValue: '9' } },
        },
      ];
      const result = applyCompositeFilter(
        docs,
        {
          op: 'AND',
          filters: [
            {
              fieldFilter: {
                field: { fieldPath: 'a' },
                op: 'EQUAL',
                value: { stringValue: '1' },
              },
            },
            {
              fieldFilter: {
                field: { fieldPath: 'b' },
                op: 'EQUAL',
                value: { stringValue: '2' },
              },
            },
          ],
        },
        logger,
        applyQueryFilters,
      );
      expect(result).to.have.length(1);
      expect(result[0].name).to.equal('p/c/d/a');
    });
    it('OR combines results', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { x: { stringValue: 'a' } } },
        { name: 'p/c/d/b', fields: { x: { stringValue: 'b' } } },
        { name: 'p/c/d/c', fields: { x: { stringValue: 'c' } } },
      ];
      const result = applyCompositeFilter(
        docs,
        {
          op: 'OR',
          filters: [
            {
              fieldFilter: {
                field: { fieldPath: 'x' },
                op: 'EQUAL',
                value: { stringValue: 'a' },
              },
            },
            {
              fieldFilter: {
                field: { fieldPath: 'x' },
                op: 'EQUAL',
                value: { stringValue: 'c' },
              },
            },
          ],
        },
        logger,
        applyQueryFilters,
      );
      expect(result).to.have.length(2);
      const names = result.map((d) => d.name).sort();
      expect(names).to.include('p/c/d/a');
      expect(names).to.include('p/c/d/c');
    });
  });

  describe('applyQueryFilters', () => {
    const logger = undefined;
    it('returns docs unchanged when no filter or empty docs', () => {
      expect(applyQueryFilters([], { fieldFilter: {} }, logger)).to.deep.equal(
        [],
      );
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { x: { stringValue: '1' } } },
      ];
      expect(applyQueryFilters(docs, null as any, logger)).to.have.length(1);
    });
    it('applies fieldFilter', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/a', fields: { k: { stringValue: 'v' } } },
        { name: 'p/c/d/b', fields: { k: { stringValue: 'w' } } },
      ];
      const out = applyQueryFilters(
        docs,
        {
          fieldFilter: {
            field: { fieldPath: 'k' },
            op: 'EQUAL',
            value: { stringValue: 'v' },
          },
        },
        logger,
      );
      expect(out).to.have.length(1);
      expect(out[0].name).to.equal('p/c/d/a');
    });
  });

  describe('applyOrderBy', () => {
    const reconstruct = (doc: FirestoreDocument) => doc.fields ?? {};
    it('sorts by field ascending', () => {
      const docs: FirestoreDocument[] = [
        { name: 'p/c/d/b', fields: { score: { integerValue: '30' } } },
        { name: 'p/c/d/a', fields: { score: { integerValue: '10' } } },
      ];
      const sorted = applyOrderBy(
        docs,
        [{ field: { fieldPath: 'score' }, direction: 1 }],
        reconstruct,
      );
      expect(sorted[0].fields?.score?.integerValue).to.equal('10');
      expect(sorted[1].fields?.score?.integerValue).to.equal('30');
    });
    it('returns docs when orderBy empty or docs empty', () => {
      const docs: FirestoreDocument[] = [{ name: 'p/c/d/a', fields: {} }];
      expect(applyOrderBy(docs, [], reconstruct)).to.have.length(1);
      expect(
        applyOrderBy([], [{ field: { fieldPath: 'x' } }], reconstruct),
      ).to.deep.equal([]);
    });
  });

  describe('inferFieldType', () => {
    it('infers arrayValue for list-like names', () => {
      expect(inferFieldType('items')).to.equal('arrayValue');
      expect(inferFieldType('tags')).to.equal('arrayValue');
    });
    it('infers mapValue for data-like names', () => {
      expect(inferFieldType('data')).to.equal('mapValue');
      expect(inferFieldType('metadata')).to.equal('mapValue');
    });
  });

  describe('detectFieldType', () => {
    it('detects type from value', () => {
      expect(detectFieldType({ stringValue: 'x' })).to.equal('stringValue');
      expect(detectFieldType({ integerValue: '1' })).to.equal('integerValue');
      expect(detectFieldType({ nullValue: null })).to.equal('nullValue');
    });
  });

  describe('generateDocumentId', () => {
    it('returns non-empty string', () => {
      const id = generateDocumentId();
      expect(id).to.be.a('string');
      expect(id.length).to.be.greaterThan(0);
    });
  });

  describe('safeNormalizeFilterValue', () => {
    it('returns nullValue for null/non-object', () => {
      expect(safeNormalizeFilterValue(null)).to.deep.equal({ nullValue: null });
      expect(safeNormalizeFilterValue(42)).to.deep.equal({ nullValue: null });
    });
    it('normalizes plain object value', () => {
      expect(safeNormalizeFilterValue({ stringValue: 'x' })).to.deep.equal({
        stringValue: 'x',
      });
    });
  });

  describe('toGrpcFields', () => {
    it('converts fields to gRPC shape', () => {
      const out = toGrpcFields({ name: { stringValue: 'A' } });
      expect(out).to.deep.equal({ name: { stringValue: 'A' } });
    });
  });
});
