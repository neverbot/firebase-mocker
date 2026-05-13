/**
 * Firebase Remote Config emulator: REST API compatible with firebase-admin
 * when FIREBASE_REMOTE_CONFIG_URL_BASE points to this server.
 */

export { RemoteConfigServer } from './server';
export type { RemoteConfigServerConfig } from './server';
export { RemoteConfigStorage } from './storage';
export type {
  StoredTemplate,
  RemoteConfigParameter,
  RemoteConfigParameterValue,
  RemoteConfigCondition,
  RemoteConfigParameterGroup,
  RemoteConfigVersion,
  UpdateResult,
} from './storage';
