import { useEffect, useState } from "react";
import { apiGet } from "./api/client";
import type { HealthResponse, ModuleInfo, StreamInfo, ToolInfo } from "./api/types";
import "./App.css";

function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [modules, setModules] = useState<ModuleInfo[]>([]);
  const [tools, setTools] = useState<ToolInfo[]>([]);
  const [streams, setStreams] = useState<StreamInfo[] | null>(null);
  const [streamsError, setStreamsError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState("overview");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const [healthData, moduleData, toolData] = await Promise.all([
          apiGet<HealthResponse>("/health"),
          apiGet<ModuleInfo[]>("/modules"),
          apiGet<ToolInfo[]>("/tools"),
        ]);

        setHealth(healthData);
        setModules(moduleData);
        setTools(toolData);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    }

    load();
  }, []);

  useEffect(() => {
    if (activeTab !== "streams" || streams !== null) return;
    apiGet<StreamInfo[]>("/streams")
      .then((value) => { setStreams(value); setStreamsError(null); })
      .catch((err: unknown) => setStreamsError(err instanceof Error ? err.message : "Unable to load streams"));
  }, [activeTab, streams]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">ST</div>
          <div>
            <h1>Smart Telemetry</h1>
            <p>Governance Platform</p>
          </div>
        </div>

        <nav>
          <button onClick={() => setActiveTab("overview")}>Overview</button>
          <button onClick={() => setActiveTab("modules")}>Modules</button>
          <button onClick={() => setActiveTab("tools")}>Tools</button>
          <button onClick={() => setActiveTab("streams")}>Streams</button>
          <button disabled>Sources</button>
          <button disabled>Topics</button>
          <button disabled>Classes</button>
          <button disabled>Duplicates</button>
          <button disabled>Dashboards</button>
          <button disabled>Query</button>
        </nav>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <h2>System Console</h2>
            <p>Core platform status and runtime registries</p>
          </div>

          <div className={`status ${health?.status === "ok" ? "ok" : "bad"}`}>
            {health?.status ?? "offline"}
          </div>
        </header>

        {error && <div className="error">API connection error: {error}</div>}

        {activeTab === "overview" && (
          <section className="grid">
            <div className="card">
              <span>Backend</span>
              <strong>{health?.status ?? "unknown"}</strong>
              <p>{health?.service ?? "No service response"}</p>
            </div>

            <div className="card">
              <span>Version</span>
              <strong>{health?.version ?? "-"}</strong>
              <p>Current backend API version</p>
            </div>

            <div className="card">
              <span>Modules</span>
              <strong>{modules.length}</strong>
              <p>Registered runtime modules</p>
            </div>

            <div className="card">
              <span>Tools</span>
              <strong>{tools.length}</strong>
              <p>Registered agent/RAG tools</p>
            </div>
          </section>
        )}

        {activeTab === "modules" && (
          <section className="panel">
            <h3>Registered Modules</h3>
            {modules.length === 0 ? (
              <p className="empty">No modules registered yet.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Module</th>
                    <th>Version</th>
                    <th>Healthy</th>
                  </tr>
                </thead>
                <tbody>
                  {modules.map((module) => (
                    <tr key={module.module_id}>
                      <td>{module.module_id}</td>
                      <td>{module.version}</td>
                      <td>{module.healthy ? "yes" : "no"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        )}

        {activeTab === "tools" && (
          <section className="panel">
            <h3>Registered Tools</h3>
            {tools.length === 0 ? (
              <p className="empty">No tools registered yet.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Tool</th>
                    <th>Description</th>
                    <th>Capabilities</th>
                  </tr>
                </thead>
                <tbody>
                  {tools.map((tool) => (
                    <tr key={tool.tool_id}>
                      <td>{tool.tool_id}</td>
                      <td>{tool.description}</td>
                      <td>{tool.capabilities.join(", ")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        )}

        {activeTab === "streams" && (
          <section className="panel"><h3>Discovered Streams</h3>
            {streamsError ? <><p className="error">Unable to load streams: {streamsError}</p><button onClick={() => { setStreamsError(null); setStreams(null); }}>Retry</button></> : streams === null ? <p className="empty">Loading streams…</p> : streams.length === 0 ? <p className="empty">No streams discovered yet.</p> : (
              <table><thead><tr><th>Stream</th><th>Source</th><th>Topic</th><th>Status</th><th>Observations</th><th>Last observed</th></tr></thead><tbody>
                {streams.map((stream) => <tr key={stream.id}><td>{stream.stream_key.slice(0, 12)}</td><td>{stream.source_id}</td><td>{stream.topic}</td><td>{stream.lifecycle_status}</td><td>{stream.observation_count}</td><td>{stream.last_observed_at}</td></tr>)}
              </tbody></table>
            )}
          </section>
        )}
      </main>
    </div>
  );
}

export default App;
