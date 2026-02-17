/**
 * Unit tests for the Commit gRPC handler (handlers/commit.ts).
 * Calls handleCommit directly so coverage is attributed to our code.
 */

import * as grpc from '@grpc/grpc-js';
import { expect } from 'chai';
import { handleCommit } from '../../src/firestore/handlers/commit';
import { buildDocumentPath } from '../../src/firestore/utils';
import { getFirestoreServer } from '../_setup';

function commitDatabase(projectId: string, databaseId: string): string {
  return `projects/${projectId}/databases/${databaseId}`;
}

describe('Firestore Commit (unit)', () => {
  const projectId = 'test-project';
  const databaseId = '(default)';

  describe('handleCommit (direct)', () => {
    it('invalid database path: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          database: 'invalid-db',
          writes: [],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'Invalid database path',
          );
          expect(value === null || value === undefined).to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('empty writes: callback with empty write_results', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.be.an('array').that.is.empty;
          expect(value?.commit_time).to.exist;
          expect(value?.commit_time?.seconds).to.exist;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('one update write: document in storage and one write_result', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'commit_update_coll';
      const docId = 'd1';
      const path = buildDocumentPath(
        projectId,
        databaseId,
        collectionId,
        docId,
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { title: { stringValue: 'FromCommit' } },
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.have.length(1);
          expect(value?.write_results?.[0]?.update_time).to.exist;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            collectionId,
            docId,
          );
          expect(stored).to.exist;
          expect(stored?.fields?.title?.stringValue).to.equal('FromCommit');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('one delete write: document removed and one write_result', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const collectionId = 'commit_delete_coll';
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
        fields: {},
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [{ delete: path }],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.have.length(1);
          const stored = storage.getDocument(
            projectId,
            databaseId,
            collectionId,
            docId,
          );
          expect(stored).to.be.null;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('invalid document path in update: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: 'invalid-path',
                fields: {},
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'Invalid document path',
          );
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('invalid document path in delete: callback with INVALID_ARGUMENT', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [{ delete: 'invalid-path' }],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INVALID_ARGUMENT);
          expect((err as { message?: string })?.message).to.include(
            'Invalid document path',
          );
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('multiple writes: update and delete in one commit', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const coll = 'commit_multi_coll';
      const path1 = buildDocumentPath(projectId, databaseId, coll, 'a');
      const path2 = buildDocumentPath(projectId, databaseId, coll, 'b');
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, coll, 'b', {
        name: path2,
        fields: {},
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            { update: { name: path1, fields: { x: { integerValue: '1' } } } },
            { delete: path2 },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.have.length(2);
          expect(storage.getDocument(projectId, databaseId, coll, 'a')).to
            .exist;
          expect(storage.getDocument(projectId, databaseId, coll, 'b')).to.be
            .null;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('writes as object (not array): Object.values path', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_obj_coll',
        'd1',
      );
      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: {
            0: {
              update: {
                name: path,
                fields: { x: { stringValue: 'from-object' } },
              },
            },
          },
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.have.length(1);
          expect(
            storage.getDocument(projectId, databaseId, 'commit_obj_coll', 'd1')
              ?.fields?.x?.stringValue,
          ).to.equal('from-object');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('update with existing doc and no updateMask: merges existing fields', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const coll = 'commit_merge_coll';
      const docId = 'd1';
      const path = buildDocumentPath(projectId, databaseId, coll, docId);
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, coll, docId, {
        name: path,
        fields: { a: { integerValue: '1' } },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { b: { stringValue: 'two' } },
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            coll,
            docId,
          );
          expect(stored?.fields?.a?.integerValue).to.equal('1');
          expect(stored?.fields?.b?.stringValue).to.equal('two');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('update with existing doc and updateMask: merges and applies mask', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const coll = 'commit_mask_coll';
      const docId = 'd1';
      const path = buildDocumentPath(projectId, databaseId, coll, docId);
      const now = new Date().toISOString();
      storage.setDocument(projectId, databaseId, coll, docId, {
        name: path,
        fields: {
          a: { stringValue: 'keep' },
          b: { stringValue: 'replace' },
          c: { stringValue: 'remove' },
        },
        createTime: now,
        updateTime: now,
      });

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { b: { stringValue: 'updated' } },
              },
              updateMask: { fieldPaths: ['a', 'b'] },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            coll,
            docId,
          );
          expect(stored?.fields?.a).to.be.undefined;
          expect(stored?.fields?.b?.stringValue).to.equal('updated');
          expect(stored?.fields?.c?.stringValue).to.equal('remove');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('update with updateTransforms REQUEST_TIME: sets timestamp field', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_transform_coll',
        'd1',
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { title: { stringValue: 'WithTime' } },
              },
              updateTransforms: [
                {
                  fieldPath: 'updatedAt',
                  setToServerValue: 'REQUEST_TIME',
                },
              ],
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            'commit_transform_coll',
            'd1',
          );
          expect(stored?.fields?.title?.stringValue).to.equal('WithTime');
          expect(stored?.fields?.updatedAt?.timestampValue).to.exist;
          expect(typeof stored?.fields?.updatedAt?.timestampValue === 'string')
            .to.be.true;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('update with transform.fieldTransforms setToServerValue 1', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_field_transform_coll',
        'd1',
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: {},
              },
              transform: {
                fieldTransforms: [
                  {
                    fieldPath: 'createdAt',
                    setToServerValue: 1,
                  },
                ],
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            'commit_field_transform_coll',
            'd1',
          );
          expect(stored?.fields?.createdAt?.timestampValue).to.exist;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('delete with write["delete"] key (alternative key)', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_delete_alt_coll',
        'd1',
      );
      const now = new Date().toISOString();
      storage.setDocument(
        projectId,
        databaseId,
        'commit_delete_alt_coll',
        'd1',
        {
          name: path,
          fields: {},
          createTime: now,
          updateTime: now,
        },
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [{ delete: path }],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          expect(
            storage.getDocument(
              projectId,
              databaseId,
              'commit_delete_alt_coll',
              'd1',
            ),
          ).to.be.null;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('updateTransforms with set_to_server_value and REQUEST_TIME_UNSPECIFIED', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_transform_snake_coll',
        'd1',
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: {},
              },
              update_transforms: [
                {
                  field_path: 'ts',
                  set_to_server_value: 'REQUEST_TIME_UNSPECIFIED',
                },
              ],
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            'commit_transform_snake_coll',
            'd1',
          );
          expect(stored?.fields?.ts?.timestampValue).to.exist;
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('fieldTransforms with field_transforms and serverValue', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const path = buildDocumentPath(
        projectId,
        databaseId,
        'commit_field_transform_snake_coll',
        'd1',
      );

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { x: { stringValue: 'y' } },
              },
              transform: {
                field_transforms: [
                  {
                    field_path: 'serverTs',
                    serverValue: 'REQUEST_TIME',
                  },
                ],
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.be.null;
          const stored = storage.getDocument(
            projectId,
            databaseId,
            'commit_field_transform_snake_coll',
            'd1',
          );
          expect(stored?.fields?.serverTs?.timestampValue).to.exist;
          expect(stored?.fields?.x?.stringValue).to.equal('y');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('document NOT found in storage after save: logs warning but callback succeeds', function (done) {
      const server = getFirestoreServer();
      const storage = server.getStorage();
      const coll = 'commit_notfound_coll';
      const docId = 'd1';
      const path = buildDocumentPath(projectId, databaseId, coll, docId);

      const originalGetDocument = storage.getDocument.bind(storage);
      let getDocumentCallCount = 0;
      (storage as any).getDocument = function (
        proj: string,
        db: string,
        c: string,
        d: string,
      ) {
        getDocumentCallCount++;
        if (getDocumentCallCount === 2 && c === coll && d === docId) {
          return null;
        }
        return originalGetDocument(proj, db, c, d);
      };

      const call = {
        request: {
          database: commitDatabase(projectId, databaseId),
          writes: [
            {
              update: {
                name: path,
                fields: { title: { stringValue: 'Saved' } },
              },
            },
          ],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, value) => {
        try {
          expect(err).to.be.null;
          expect(value?.write_results).to.have.length(1);
          const stored = originalGetDocument(
            projectId,
            databaseId,
            coll,
            docId,
          );
          expect(stored).to.exist;
          expect(stored?.fields?.title?.stringValue).to.equal('Saved');
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });

    it('catch block: callback with INTERNAL when request throws', function (done) {
      const server = getFirestoreServer();
      const call = {
        request: {
          get database() {
            throw new Error('Intentional throw');
          },
          writes: [],
        },
      } as grpc.ServerUnaryCall<any, any>;
      const callback: grpc.sendUnaryData<any> = (err, _value) => {
        try {
          expect(err).to.exist;
          expect(err?.code).to.equal(grpc.status.INTERNAL);
          expect((err as { message?: string })?.message).to.include(
            'Intentional throw',
          );
          done();
        } catch (e) {
          done(e as Error);
        }
      };
      handleCommit(server, call, callback);
    });
  });
});
