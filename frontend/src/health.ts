export interface HealthResponse {
  status: string;
  service: string;
}

export async function fetchHealth(signal: AbortSignal): Promise<HealthResponse> {
  const response = await fetch("/health", {
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal,
  });

  if (!response.ok) {
    throw new Error(`后端响应异常（HTTP ${response.status}）`);
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new Error("后端响应格式异常");
  }

  if (!isHealthResponse(payload)) {
    throw new Error("后端响应格式异常");
  }
  return payload;
}

function isHealthResponse(value: unknown): value is HealthResponse {
  if (!value || typeof value !== "object") return false;
  const payload = value as Partial<HealthResponse>;
  return payload.status === "ok" && typeof payload.service === "string" && payload.service.length > 0;
}
