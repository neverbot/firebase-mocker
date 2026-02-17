/**
 * Unit tests for the ListCollectionIds gRPC handler (handlers/listCollectionIds.ts).
 * Calls handleListCollectionIds directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleListCollectionIds } from '../../src/firestore/handlers/listCollectionIds';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function listCollectionIdsParent(
  projectId: string,
  databaseId: string,
  pathAfterDocuments = '',
): string {
  const base = `projects/${projectId}/databases/${databaseId}/documents`;
  return pathAfterDocuments ? `${base}/${pathAfterDocuments}` : base;
}

describe('Firestore handler: ListCollectionIds (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleListCollectionIds (direct)', () => {
    it('invalid parent path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: { parent: 'invalid-parent' },
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
      handleListCollectionIds(server, call, callback);
    });

    it('document parent with no subcollections: callback with empty collectionIds', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const pathAfterDocuments = 'no_subs_coll/no_subs_doc';
      const parent = listCollectionIdsParent(
        projectId,
        databaseId,
        pathAfterDocuments,
      );
      const docPath = buildDocumentPath(
        projectId,
        databaseId,
        'no_subs_coll',
        'no_subs_doc',
      );
      const now = new Date().toISOString();
      storage.setDocument(
        projectId,
        databaseId,
        'no_subs_coll',
        'no_subs_doc',
        {
          name: docPath,
          fields: {},
          createTime: now,
          updateTime: now,
        },
      );

      const call = {
        request: { parent },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.collectionIds).to.be.an('array').that.is.empty;
          expect(value?.nextPageToken).to.equal('');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleListCollectionIds(server, call, callback);
    });

    it('root parent with collections: callback includes expected collection ids', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const now = new Date().toISOString();
      const path1 = buildDocumentPath(
        projectId,
        databaseId,
        'list_ids_a',
        'd1',
      );
      const path2 = buildDocumentPath(
        projectId,
        databaseId,
        'list_ids_b',
        'd1',
      );
      storage.setDocument(projectId, databaseId, 'list_ids_a', 'd1', {
        name: path1,
        fields: {},
        createTime: now,
        updateTime: now,
      });
      storage.setDocument(projectId, databaseId, 'list_ids_b', 'd1', {
        name: path2,
        fields: {},
        createTime: now,
        updateTime: now,
      });

      const parent = listCollectionIdsParent(projectId, databaseId);
      const call = {
        request: { parent },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.collectionIds).to.be.an('array');
            expect(value?.collectionIds).to.include.members([
              'list_ids_a',
              'list_ids_b',
            ]);
            expect(value?.nextPageToken).to.equal('');
            resolve();
          } catch (e) {
            reject(e as Error);
          }
        };
        handleListCollectionIds(server, call, callback);
      });
    });

    it('pagination: pageSize and nextPageToken under document', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const pathAfterDocuments = 'page_parent/page_doc';
      const now = new Date().toISOString();
      for (const subId of ['sub1', 'sub2', 'sub3']) {
        const fullPath = `${pathAfterDocuments}/${subId}`;
        storage.setDocument(projectId, databaseId, fullPath, 'd1', {
          name: buildDocumentPath(projectId, databaseId, fullPath, 'd1'),
          fields: {},
          createTime: now,
          updateTime: now,
        });
      }

      const parent = listCollectionIdsParent(
        projectId,
        databaseId,
        pathAfterDocuments,
      );
      const callFirst = {
        request: { parent, pageSize: 2 },
      } as grpc.ServerUnaryCall<any, any>;
      const first = await new Promise<any>((resolve, reject) => {
        handleListCollectionIds(server, callFirst, (err, value) => {
          if (err) {
            reject(err as Error);
          } else {
            resolve(value);
          }
        });
      });
      expect(first.collectionIds).to.have.length(2);
      expect(first.nextPageToken).to.equal('2');

      const callNext = {
        request: { parent, pageSize: 2, pageToken: '2' },
      } as grpc.ServerUnaryCall<any, any>;
      const next = await new Promise<any>((resolve, reject) => {
        handleListCollectionIds(server, callNext, (err, value) => {
          if (err) {
            reject(err as Error);
          } else {
            resolve(value);
          }
        });
      });
      expect(next.collectionIds).to.have.length(1);
      expect(next.nextPageToken).to.equal('');
    });

    it('under document: returns subcollection ids', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const parentCol = 'parents';
      const parentId = 'p1';
      const pathAfterDocuments = `${parentCol}/${parentId}`;
      const now = new Date().toISOString();
      const childPath = buildDocumentPath(
        projectId,
        databaseId,
        `${pathAfterDocuments}/children`,
        'c1',
      );
      const itemsPath = buildDocumentPath(
        projectId,
        databaseId,
        `${pathAfterDocuments}/items`,
        'i1',
      );
      storage.setDocument(
        projectId,
        databaseId,
        `${pathAfterDocuments}/children`,
        'c1',
        {
          name: childPath,
          fields: {},
          createTime: now,
          updateTime: now,
        },
      );
      storage.setDocument(
        projectId,
        databaseId,
        `${pathAfterDocuments}/items`,
        'i1',
        {
          name: itemsPath,
          fields: {},
          createTime: now,
          updateTime: now,
        },
      );

      const parent = listCollectionIdsParent(
        projectId,
        databaseId,
        pathAfterDocuments,
      );
      const call = {
        request: { parent },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value?.collectionIds).to.have.length(2);
            expect(value?.collectionIds?.sort()).to.deep.equal([
              'children',
              'items',
            ]);
            resolve();
          } catch (e) {
            reject(e as Error);
          }
        };
        handleListCollectionIds(server, call, callback);
      });
    });
  });
});
