/**
 * Unit tests for the ListDocuments gRPC handler (handlers/listDocuments.ts).
 * Calls handleListDocuments directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleListDocuments } from '../../src/firestore/handlers/listDocuments';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function listDocumentsParent(projectId: string, databaseId: string): string {
  return `projects/${projectId}/databases/${databaseId}/documents`;
}

describe('Firestore ListDocuments (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleListDocuments (direct)', () => {
    it('invalid parent path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: { parent: 'invalid-parent', collectionId: 'c' },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'Invalid parent path',
          );
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleListDocuments(server, call, callback);
    });

    it('missing collectionId: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const parent = listDocumentsParent(projectId, databaseId);
      const call = {
        request: { parent, collectionId: '' },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'collectionId is required',
          );
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleListDocuments(server, call, callback);
    });

    it('empty collection: callback with documents array empty', function (done) {
      const server = getFirestoreServer();
      const parent = listDocumentsParent(projectId, databaseId);
      const collectionId = 'list_empty_coll';
      const call = {
        request: { parent, collectionId },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value).to.exist;
          expect(value?.documents).to.be.an('array').that.is.empty;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleListDocuments(server, call, callback);
    });

    it('collection with documents: callback with documents and correct names', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'list_docs_coll';
      const parent = listDocumentsParent(projectId, databaseId);

      const doc1Path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        'doc1',
      );
      const doc2Path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        'doc2',
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionId, 'doc1', {
        name: doc1Path,
        fields: { title: { stringValue: 'First' } },
        createTime: now,
        updateTime: now,
      });
      storage.setDocument(projectId, databaseId, collectionId, 'doc2', {
        name: doc2Path,
        fields: { title: { stringValue: 'Second' } },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: { parent, collectionId },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.documents).to.have.length(2);
            const names = (value?.documents as any[]).map((d) => d.name).sort();
            expect(names).to.include(doc1Path);
            expect(names).to.include(doc2Path);
            resolve();
          } catch (e) {
            reject(e as Error);
          }
        };
        handleListDocuments(server, call, callback);
      });
    });

    it('subcollection: list documents with correct parent path', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const parentCol = 'parents';
      const parentId = 'p1';
      const subCol = 'children';
      const childId = 'c1';
      const parent = `${listDocumentsParent(projectId, databaseId)}/${parentCol}/${parentId}`;
      const collectionPath = `${parentCol}/${parentId}/${subCol}`;
      const childPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionPath,
        childId,
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionPath, childId, {
        name: childPath,
        fields: { name: { stringValue: 'Child' } },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: { parent, collectionId: subCol },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.documents).to.have.length(1);
            expect((value?.documents as any[])[0].name).to.equal(childPath);
            resolve();
          } catch (e) {
            reject(e as Error);
          }
        };
        handleListDocuments(server, call, callback);
      });
    });
  });
});
