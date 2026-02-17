/**
 * Unit tests for the GetDocument gRPC handler (handlers/getDocument.ts).
 * Calls handleGetDocument directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleGetDocument } from '../../src/firestore/handlers/getDocument';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

describe('Firestore GetDocument (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleGetDocument (direct)', () => {
    it('invalid path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: { name: 'invalid-path' },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect(err?.message).to.include('Invalid document path');
          expect(value == null).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleGetDocument(server, call, callback);
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
          expect(err?.message).to.include('Document not found');
          expect(value == null).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleGetDocument(server, call, callback);
    });

    it('document exists: callback with document (success path)', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'direct_get_coll';
      const docId = 'direct-doc';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, collectionId, docId, {
        name: path,
        fields: {
          name: { stringValue: 'Direct' },
          count: { integerValue: '42' },
        },
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
            expect(value?.name).to.equal(path);
            expect(value?.fields?.name?.stringValue).to.equal('Direct');
            expect(value?.fields?.count?.integerValue).to.equal('42');
            expect(value?.createTime).to.exist;
            expect(value?.updateTime).to.exist;
            resolve();
          } catch (e) {
            reject(e);
          }
        };
        handleGetDocument(server, call, callback);
      });
    });

    it('document with missing createTime/updateTime: uses default timestamps', async function () {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'direct_get_coll';
      const docId = 'no-times-doc';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );
      storage.setDocument(projectId, databaseId, collectionId, docId, {
        name: path,
        fields: { x: { stringValue: 'y' } },
        // no createTime / updateTime
      });

      const call = {
        request: { name: path },
      } as grpc.ServerUnaryCall<any, any>;
      await new Promise<void>((resolve, reject) => {
        const callback: grpc.sendUnaryData<any> = (err, value) => {
          try {
            expect(err).to.be.null;
            expect(value).to.exist;
            expect(value?.createTime).to.exist;
            expect(value?.updateTime).to.exist;
            resolve();
          } catch (e) {
            reject(e);
          }
        };
        handleGetDocument(server, call, callback);
      });
    });
  });
});
