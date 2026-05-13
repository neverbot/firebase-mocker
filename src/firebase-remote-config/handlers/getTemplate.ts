import { Request, Response } from 'express';
import { RemoteConfigStorage } from '../storage';

export function handleGetTemplate(
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

  const t = storage.getTemplate();
  res.set('ETag', t.etag);
  res.status(200).json({
    parameters: t.parameters,
    conditions: t.conditions,
    parameterGroups: t.parameterGroups,
    version: t.version,
  });
}
