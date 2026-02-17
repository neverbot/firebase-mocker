/**
 * Unit tests for the UpdateDocument gRPC handler (handlers/updateDocument.ts).
 * Calls handleUpdateDocument directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleUpdateDocument } from '../../src/firestore/handlers/updateDocument';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

describe('Firestore UpdateDocument (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleUpdateDocument (direct)', () => {
    it('invalid path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          document: { name: 'invalid-path', fields: {} },
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'Invalid document path',
          );
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleUpdateDocument(server, call, callback);
    });

    it('update non-existent document (upsert): creates doc and callback with document', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'update_upsert_coll';
      const docId = 'upserted-doc';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      const call = {
        request: {
          document: {
            name: path,
            fields: { title: { stringValue: 'Upserted' } },
          },
        },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.name).to.equal(path);
            expect(value?.fields?.title?.stringValue).to.equal('Upserted');
            expect(value?.createTime).to.exist;
            expect(value?.updateTime).to.exist;
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleUpdateDocument(server, call, callback);
      });

      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.fields?.title?.stringValue).to.equal('Upserted');
    });

    it('update existing document: preserves createTime, new updateTime and fields', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'update_existing_coll';
      const docId = 'existing-doc';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      const originalCreate = '2020-01-01T00:00:00.000Z';
      storage.setDocument(projectId, databaseId, collectionId, docId, {
        name: path,
        fields: { old: { stringValue: 'value' } },
        createTime: originalCreate,
        updateTime: originalCreate,
      });

      const call = {
        request: {
          document: {
            name: path,
            fields: {
              old: { stringValue: 'value' },
              newField: { stringValue: 'added' },
            },
          },
        },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.fields?.newField?.stringValue).to.equal('added');
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleUpdateDocument(server, call, callback);
      });

      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.createTime).to.equal(originalCreate);
      expect(stored?.updateTime).to.not.equal(originalCreate);
      expect(stored?.fields?.newField?.stringValue).to.equal('added');
    });

    it('update document in subcollection: success with correct path', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionPath = 'parents/p1/children';
      const docId = 'c1';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionPath,
        docId,
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionPath, docId, {
        name: path,
        fields: { name: { stringValue: 'Old' } },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: {
          document: {
            name: path,
            fields: { name: { stringValue: 'Updated' } },
          },
        },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.fields?.name?.stringValue).to.equal('Updated');
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleUpdateDocument(server, call, callback);
      });

      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionPath,
        docId,
      );
      expect(stored?.fields?.name?.stringValue).to.equal('Updated');
    });
  });
});
