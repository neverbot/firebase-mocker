import { Request, Response } from 'express';
import { getLogger } from '../../logger';
import { RemoteConfigStorage } from '../storage';

export function handleUpdateTemplate(
  storage: RemoteConfigStorage,
  configuredProjectId: string,
  req: Request,
  res: Response,
): void {
  const { projectId } = req.params;
  if (projectId !== configuredProjectId) {
    res
      .status(404)
      .json({ error: { code: 404, message: 'Project not found' } });
    return;
  }

  if (req.query.validate_only === 'true') {
    getLogger().warn(
      'server',
      '[REMOTE-CONFIG] validate_only=true is not implemented (returning 501)',
    );
    res.status(501).json({
      error: {
        code: 501,
        status: 'UNIMPLEMENTED',
        message: 'validate_only is not implemented in firebase-mocker',
      },
    });
    return;
  }

  const ifMatch = req.header('if-match');
  if (!ifMatch) {
    res.status(400).json({
      error: { code: 400, message: 'Missing If-Match header' },
    });
    return;
  }

  const body = (req.body || {}) as Record<string, unknown>;
  const result = storage.updateTemplate(body, ifMatch);

  if (!result.ok) {
    res.status(412).json({
      error: {
        code: 412,
        status: 'FAILED_PRECONDITION',
        message: 'ETag mismatch',
      },
    });
    return;
  }

  res.set('ETag', result.template.etag);
  res.status(200).json({
    parameters: result.template.parameters,
    conditions: result.template.conditions,
    parameterGroups: result.template.parameterGroups,
    version: result.template.version,
  });
}
