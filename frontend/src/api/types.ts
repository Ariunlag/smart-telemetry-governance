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