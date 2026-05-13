import { Request, Response } from 'express';
import { config } from '../../config';
import { getLogger } from '../../logger';

export function handleUnimplemented(req: Request, res: Response): void {
  const policy = config.getString('logs.onUnimplemented', 'warn');
  const message = `Remote Config endpoint ${req.method} ${req.path} is not implemented in firebase-mocker`;
  getLogger().warn('server', `[REMOTE-CONFIG] ${message}`);

  if (policy === 'throw') {
    throw new Error(message);
  }

  res.status(501).json({
    error: { code: 501, status: 'UNIMPLEMENTED', message },
  });
}
