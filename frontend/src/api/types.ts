export type HealthResponse = {
  status: string;
  version: string;
  service: string;
};

export type ModuleInfo = {
  module_id: string;
  version: string;
  healthy: boolean;
};

export type ToolInfo = {
  tool_id: string;
  description: string;
  capabilities: string[];
};

export type StreamInfo = {
  id: string; stream_key: string; source_id: string; topic: string; lifecycle_status: string;
  observation_count: number; last_observed_at: string;
};
