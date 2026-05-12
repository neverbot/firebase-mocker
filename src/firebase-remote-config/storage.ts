/**
 * In-memory storage for Firebase Remote Config emulator.
 * Tracks a single template + monotonically-increasing etag/version counter.
 */

export interface RemoteConfigParameterValue {
  value?: string;
  useInAppDefault?: boolean;
  personalizationValue?: { personalizationId: string };
}

export interface RemoteConfigParameter {
  defaultValue?: RemoteConfigParameterValue;
  conditionalValues?: Record<string, RemoteConfigParameterValue>;
  description?: string;
  valueType?:
    | 'PARAMETER_VALUE_TYPE_UNSPECIFIED'
    | 'STRING'
    | 'BOOLEAN'
    | 'NUMBER'
    | 'JSON';
}

export interface RemoteConfigCondition {
  name: string;
  expression: string;
  tagColor?: string;
}

export interface RemoteConfigParameterGroup {
  description?: string;
  parameters?: Record<string, RemoteConfigParameter>;
}

export interface RemoteConfigVersion {
  versionNumber: string;
  updateTime: string;
  updateOrigin: 'ADMIN_SDK_NODE';
  updateType: 'INCREMENTAL_UPDATE';
}

export interface StoredTemplate {
  parameters: Record<string, RemoteConfigParameter>;
  conditions: RemoteConfigCondition[];
  parameterGroups: Record<string, RemoteConfigParameterGroup>;
  version: RemoteConfigVersion;
  etag: string;
}

export type UpdateResult =
  | { ok: true; template: StoredTemplate }
  | { ok: false; currentEtag: string };

export class RemoteConfigStorage {
  private template: StoredTemplate;
  private etagCounter: number;
  private readonly projectId: string;

  constructor(projectId: string, initialTemplate?: Partial<StoredTemplate>) {
    this.projectId = projectId;
    if (initialTemplate) {
      this.etagCounter = 1;
      this.template = {
        parameters: initialTemplate.parameters ?? {},
        conditions: initialTemplate.conditions ?? [],
        parameterGroups: initialTemplate.parameterGroups ?? {},
        version: {
          versionNumber: '1',
          updateTime: new Date().toISOString(),
          updateOrigin: 'ADMIN_SDK_NODE',
          updateType: 'INCREMENTAL_UPDATE',
        },
        etag: `etag-${projectId}-1`,
      };
    } else {
      this.etagCounter = 0;
      this.template = {
        parameters: {},
        conditions: [],
        parameterGroups: {},
        version: {
          versionNumber: '0',
          updateTime: new Date().toISOString(),
          updateOrigin: 'ADMIN_SDK_NODE',
          updateType: 'INCREMENTAL_UPDATE',
        },
        etag: `etag-${projectId}-0`,
      };
    }
  }

  getTemplate(): StoredTemplate {
    return this.template;
  }

  updateTemplate(body: Partial<StoredTemplate>, ifMatch: string): UpdateResult {
    if (ifMatch !== '*' && ifMatch !== this.template.etag) {
      return { ok: false, currentEtag: this.template.etag };
    }
    this.etagCounter += 1;
    const newEtag = `etag-${this.projectId}-${this.etagCounter}`;
    this.template = {
      parameters: body.parameters ?? {},
      conditions: body.conditions ?? [],
      parameterGroups: body.parameterGroups ?? {},
      version: {
        versionNumber: String(this.etagCounter),
        updateTime: new Date().toISOString(),
        updateOrigin: 'ADMIN_SDK_NODE',
        updateType: 'INCREMENTAL_UPDATE',
      },
      etag: newEtag,
    };
    return { ok: true, template: this.template };
  }

  clear(): void {
    this.etagCounter = 0;
    this.template = {
      parameters: {},
      conditions: [],
      parameterGroups: {},
      version: {
        versionNumber: '0',
        updateTime: new Date().toISOString(),
        updateOrigin: 'ADMIN_SDK_NODE',
        updateType: 'INCREMENTAL_UPDATE',
      },
      etag: `etag-${this.projectId}-0`,
    };
  }
}
