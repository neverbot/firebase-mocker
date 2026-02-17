/**
 * Unit tests for the DeleteDocument gRPC handler (handlers/deleteDocument.ts).
 * Calls handleDeleteDocument directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleDeleteDocument } from '../../src/firestore/handlers/deleteDocument';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

describe('Firestore DeleteDocument (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleDeleteDocument (direct)', () => {
    it('invalid path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: { name: 'invalid-path' },
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
      handleDeleteDocument(server, call, callback);
    });

    it('document not found: callback with NOT_FOUND', function (done) {
      const server = getFirestoreServer();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'nonexistent_coll',
        'nonexistent_doc',
      );
      const call = {
        request: { name: path },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.NOT_FOUND);
          expect((err as { message?: string })?.message).to.include(
            'Document not found',
          );
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleDeleteDocument(server, call, callback);
    });

    it('document exists: callback with success and document removed from storage', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'delete_direct_coll';
      const docId = 'to-delete';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionId, docId, {
        name: path,
        fields: { x: { stringValue: 'y' } },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: { name: path },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value).to.exist;
            expect(value).to.deep.equal({});
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleDeleteDocument(server, call, callback);
      });

      const afterDelete = storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      expect(afterDelete).to.be.null;
    });

    it('subcollection document: deletes by full path', async function () {
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
        fields: {},
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: { name: path },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value).to.deep.equal({});
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleDeleteDocument(server, call, callback);
      });

      expect(storage.getDocument(projectId, databaseId, collectionPath, docId))
        .to.be.null;
    });
  });
});
