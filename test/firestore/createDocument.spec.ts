/**
 * Unit tests for the CreateDocument gRPC handler (handlers/createDocument.ts).
 * Calls handleCreateDocument directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleCreateDocument } from '../../src/firestore/handlers/createDocument';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function createDocumentParent(
  projectId: string,
  databaseId: string,
  pathAfterDocuments = '',
): string {
  const base = `projects/${projectId}/databases/${databaseId}/documents`;
  return pathAfterDocuments ? `${base}/${pathAfterDocuments}` : base;
}

describe('Firestore CreateDocument (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleCreateDocument (direct)', () => {
    it('invalid parent path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          parent: 'invalid-parent',
          collectionId: 'c',
          documentId: 'd1',
        },
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
      handleCreateDocument(server, call, callback);
    });

    it('create in root collection with documentId and fields: success and doc in storage', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'create_direct_coll';
      const docId = 'created-doc';
      const parent = createDocumentParent(projectId, databaseId);
      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      const call = {
        request: {
          parent,
          collectionId,
          documentId: docId,
          document: {
            fields: {
              title: { stringValue: 'Hello' },
              count: { integerValue: '42' },
            },
          },
        },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.name).to.equal(documentPath);
            expect(value?.fields?.title?.stringValue).to.equal('Hello');
            expect(value?.fields?.count?.integerValue).to.equal('42');
            expect(value?.createTime).to.exist;
            expect(value?.updateTime).to.exist;
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleCreateDocument(server, call, callback);
      });

      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.name).to.equal(documentPath);
      expect(stored?.fields?.title?.stringValue).to.equal('Hello');
    });

    it('create with auto-generated documentId: success and doc has generated id', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'create_auto_coll';
      const parent = createDocumentParent(projectId, databaseId);

      const call = {
        request: {
          parent,
          collectionId,
          document: { fields: { x: { stringValue: 'y' } } },
        },
      } as grpc.ServerUnaryCall<any, any>;
      let createdName: string | undefined;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.name).to.exist;
            expect(value?.name).to.include(projectId);
            expect(value?.name).to.include(collectionId);
            createdName = value?.name;
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleCreateDocument(server, call, callback);
      });

      expect(createdName).to.exist;
      const docId = createdName!.split('/').pop();
      expect(docId).to.exist;
      expect(docId).to.have.lengthOf.above(0);
      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionId,
        docId!,
      );
      expect(stored).to.exist;
    });

    it('create in subcollection: success with correct path', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const parentCol = 'parents';
      const parentId = 'p1';
      const subCol = 'children';
      const docId = 'c1';
      const pathAfterDocuments = `${parentCol}/${parentId}`;
      const parent = createDocumentParent(
        projectId,
        databaseId,
        pathAfterDocuments,
      );
      const collectionPath = `${pathAfterDocuments}/${subCol}`;
      const documentPath = buildDocumentPath(
        projectId,
        databaseId,
        collectionPath,
        docId,
      );

      const call = {
        request: {
          parent,
          collectionId: subCol,
          documentId: docId,
          document: { fields: { name: { stringValue: 'Child' } } },
        },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.name).to.equal(documentPath);
            expect(value?.fields?.name?.stringValue).to.equal('Child');
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        };
        handleCreateDocument(server, call, callback);
      });

      const stored = storage.getDocument(
        projectId,
        databaseId,
        collectionPath,
        docId,
      );
      expect(stored).to.exist;
      expect(stored?.name).to.equal(documentPath);
    });
  });
});
