import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchHealth, type HealthResponse } from "./health";

type HealthState =
  | { kind: "checking" }
  | { kind: "connected"; data: HealthResponse }
  | { kind: "error"; message: string };

const requestTimeoutMs = 5_000;

export function App() {
  const [health, setHealth] = useState<HealthState>({ kind: "checking" });
  const serviceAddress = useMemo(() => new URL("/health", window.location.origin).toString(), []);

  const checkHealth = useCallback(async () => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), requestTimeoutMs);
    setHealth({ kind: "checking" });

    try {
      const data = await fetchHealth(controller.signal);
      setHealth({ kind: "connected", data });
    } catch (error) {
      const message = error instanceof DOMException && error.name === "AbortError"
        ? "连接后端超时，请确认服务已启动。"
        : error instanceof TypeError
          ? "无法连接后端，请确认服务已启动并检查服务地址。"
          : error instanceof Error
            ? error.message
            : "后端连接失败，请稍后重试。";
      setHealth({ kind: "error", message });
    } finally {
      window.clearTimeout(timeout);
    }
  }, []);

  useEffect(() => {
    void checkHealth();
  }, [checkHealth]);

  const statusLabel = health.kind === "checking"
    ? "检查中"
    : health.kind === "connected"
      ? "已连接"
      : "连接失败";

  return (
    <main className="page-shell">
      <section className="status-card" aria-labelledby="page-title">
        <div className="brand-mark" aria-hidden="true">IM</div>
        <p className="eyebrow">Web 即时通讯基础工程</p>
        <h1 id="page-title">IM</h1>
        <p className="intro">先确认前后端基础链路是否正常。</p>

        <dl className="status-list">
          <div className="status-row">
            <dt>前端</dt>
            <dd><span className="status-dot status-dot--ok" />已加载</dd>
          </div>
          <div className="status-row">
            <dt>后端</dt>
            <dd aria-live="polite">
              <span className={`status-dot status-dot--${health.kind === "connected" ? "ok" : health.kind === "error" ? "error" : "checking"}`} />
              {statusLabel}
            </dd>
          </div>
          <div className="status-row status-row--address">
            <dt>服务地址</dt>
            <dd><code>{serviceAddress}</code></dd>
          </div>
        </dl>

        {health.kind === "connected" && (
          <p className="feedback feedback--success" role="status">
            后端服务已响应（{health.data.service}）。
          </p>
        )}
        {health.kind === "error" && (
          <p className="feedback feedback--error" role="alert">{health.message}</p>
        )}

        <button className="check-button" disabled={health.kind === "checking"} onClick={() => void checkHealth()} type="button">
          {health.kind === "checking" ? "检查中…" : "重新检查"}
        </button>
      </section>
    </main>
  );
}
