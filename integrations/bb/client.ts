export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };
export type Harness = "codex" | "pi" | "cursor" | "claude-code" | "fx" | "opencode";
export type CodexAuthStatus = {
  state: "missing" | "waiting" | "connected" | "limited" | "unavailable" | "error" | "expired";
  email: string | null; plan: string | null; message: string | null;
  login_id: string | null; verification_url: string | null; user_code: string | null;
};
export type TeleportFile = { path: string; size: number; sha256: string; kind: "native" | "context" | "project" | "attachment"; executable: boolean; symlink?: boolean; origin?: string };
export type TeleportManifest = {
  request_id: string; harness: Harness; native_id: string; model: string; provider: string | null; reasoning: string | null;
  workspace: string; workspace_name: string; files: TeleportFile[]; handoff: string;
  service_tier?: string | null; command_guard_enabled?: boolean; strip_ai_co_authors?: boolean; system_prompt?: string;
  queued: (string | { text: string; reasoning?: string; service_tier?: string })[];
  /** A fork starts idle with no handoff; its first message is a `rewind` with `fork`. */
  fork?: boolean;
};
export type TeleportStatus = {
  request_id: string; session_id: string | null; phase: "uploading" | "running" | "complete" | "cancelled";
  output_started: boolean; error: string | null; error_detail?: string | null; workspace: string;
  files: { index: number; offset: number; complete: boolean; path: string | null }[];
};
export type TeleportCheck = { harness: Harness; model: string; reasoning: string | null; service_tier: string | null };
export type TeleportCheckResult = { ok: true } | {
  ok: false; code: "harness_not_configured" | "login_required" | "models_unavailable" | "model_unavailable" | "setting_unavailable" | "probe_failed";
  error: string; models: { model: string; reasoning_levels: string[] }[] | null;
};
export type Workspace = { id: string; path: string; parent?: string };
type Upload = { body: ReadableStream<Uint8Array>; length: number; contentType?: string };
/** Hex-encoded bytes keep binary input and output intact. */
export type VmRun = { command: string; stdin?: string; cwd?: string };
export type VmRunResult = { code: number | null; stdout: string; stderr: string; truncated: boolean };
export type Receipt = {
  request_id: string;
  command: string;
  state: string;
  input: Json;
  model?: string;
  provider?: string;
  error?: string;
};
export type Acceptance = { session_id: string; receipt: Receipt; saving: Json };
export type SessionRecord = {
  sequence: number;
  session_id: string;
  kind: string;
  data: Json;
  native?: string;
  timestamp_ms?: number;
};
export type Session = {
  session_id: string;
  harness: Harness;
  state: string;
  native_id: string | null;
  current_request: string | null;
  last_sequence: number;
  queue: string[];
  receipts: { [id: string]: Receipt };
};
export type SessionSummary = {
  session_id: string;
  harness: Harness;
  model: string | null;
  provider: string | null;
  state: string;
  workspace: Workspace | null;
  parent_session: string | null;
  current_request: string | null;
  queued: number;
  last_sequence: number;
  last_activity_ms: number | null;
};

const rejectionMessages: Record<string, string> = {
  cursor_auth_required: "Connect your Cursor account to use Cursor in Cloud. Your prompt is saved.",
  cursor_auth_unavailable: "Could not verify the cloud Cursor account. Check the connection and try again.",
  cursor_auth_busy: "Close Cursor sessions before changing the cloud login. Existing threads were not changed.",
  teleport_rejected: "Teleport could not validate its saved conversation or files. The source is preserved. Check the transfer details and core logs before retrying.",
  teleport_cancelled: "This transfer was cancelled. Local history is preserved.",
  teleport_running: "Cloud execution already owns this transfer. Use Stop instead of Cancel.",
  codex_auth_required: "Connect your ChatGPT subscription to use Codex in Cloud. Your prompt is saved.",
  claude_auth_required: "Connect your Claude subscription to use Claude Code in Cloud. Your prompt is saved.",
  claude_auth_unavailable: "Could not verify the cloud Claude account. Check the CLI installation and native login, then retry.",
  codex_auth_unavailable: "Could not verify the cloud Codex account. Check the connection and try again.",
  codex_usage_limit: "Your Codex usage limit is reached. Sign in with another ChatGPT account to keep working, or wait for it to reset.",
  codex_auth_busy: "Finish active Codex work before signing in. Running threads were not changed.",
  invalid_provider: "Select a valid inference provider for Pi.",
  invalid_model: "This model is unavailable on Cloud. Refresh the model selection.",
  invalid_reasoning_effort: "This reasoning level is unavailable for the cloud model. Select a supported level.",
  request_conflict: "This request ID belongs to different content. Start a new request.",
  harness_not_configured: "Configure the selected harness on Cloud before starting.",
  invalid_workspace: "The cloud folder could not be selected. Check its name and saved mapping.",
  storage_blocked: "Cloud storage is temporarily blocking new work. Your request will retry.",
  service_stopping: "Cloud is restarting. Your request will retry.",
  service_draining: "The cloud sandbox is going to sleep. Your request will wake it and retry.",
  model_catalog_unavailable: "Cloud model discovery is unavailable. Your request will retry.",
  invalid_service_tier: "This service tier is unavailable for the cloud model.",
  invalid_attachment: "The attachment could not be stored on Cloud.",
  attachment_permission_denied: "Cloud folder permission denied",
  attachment_too_large: "The attachment exceeds the Cloud size limit (10 MiB per image, 25 MiB per file).",
};
// Thread errors with these messages ask the user to connect an account, not retry blindly.
export const authRequiredMessages = new Set([rejectionMessages.codex_auth_required, rejectionMessages.cursor_auth_required, rejectionMessages.claude_auth_required]);
const transientRejections = new Set(["storage_blocked", "service_stopping", "service_draining", "model_catalog_unavailable", "codex_auth_unavailable", "claude_auth_unavailable", "cursor_auth_unavailable"]);

export class CloudroomError extends Error {
  readonly status: number | null;
  readonly code: string | null;
  readonly retryable: boolean;

  constructor(message: string, status: number | null = null, code: string | null = null) {
    super(message);
    this.name = "CloudroomError";
    this.status = status;
    this.code = code;
    this.retryable = code && Object.hasOwn(rejectionMessages, code)
      ? transientRejections.has(code)
      : status === null || status >= 500 || [408, 409, 429].includes(status);
  }
}

export class CloudroomConnectionError extends CloudroomError {
  readonly networkCode: string | null;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "CloudroomConnectionError";
    const detail = cause instanceof Error && cause.cause ? cause.cause : cause;
    const code = detail && typeof detail === "object" && "code" in detail ? detail.code : null;
    this.networkCode = typeof code === "string" && [
      "ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "ENOTFOUND", "EAI_AGAIN",
      "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT",
    ].includes(code) ? code : null;
  }
}

/** Reads the core's `{ code, error }` body. The error text is shown to the user (ADR 0123). */
async function rejection(response: Response): Promise<{ code: string | null; error: string | null }> {
  const none = { code: null, error: null };
  const reader = response.body?.getReader();
  if (!reader) return none;
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > 4096) return none;
      chunks.push(value);
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
    const text = new TextDecoder().decode(bytes);
    let body: { code?: unknown; error?: unknown } | null = null;
    try { body = JSON.parse(text); } catch { return { code: null, error: text.trim().slice(0, 500) || null }; }
    const error = typeof body?.error === "string" ? body.error.slice(0, 500) : null;
    if (typeof body?.code === "string" && Object.hasOwn(rejectionMessages, body.code)) return { code: body.code, error };
    const legacy: Record<string, string> = {
      "invalid reasoning effort": "invalid_reasoning_effort", "invalid model": "invalid_model",
      "storage unsafe; new execution is blocked": "storage_blocked", "service is stopping": "service_stopping", "service is draining": "service_draining",
    };
    return { code: error !== null && Object.hasOwn(legacy, error) ? legacy[error] : null, error };
  } catch { return none; }
  finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

/** fetch() hides the network cause (ECONNREFUSED, DNS, TLS) in `error.cause`. */
function describe(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const cause = error.cause instanceof Error ? error.cause : null;
  const code = cause && "code" in cause && typeof cause.code === "string" ? `${cause.code} ` : "";
  return cause ? `${error.message}: ${code}${cause.message}` : error.message;
}

function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new CloudroomError("Invalid Cloudroom response");
  }
  return value as Record<string, unknown>;
}

function text(value: unknown): string {
  if (typeof value !== "string")
    throw new CloudroomError("Invalid Cloudroom response");
  return value;
}

function sequence(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new CloudroomError("Invalid Cloudroom sequence");
  }
  return value;
}

function json(value: unknown): Json {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const items = Array.isArray(value) ? value : Object.values(object(value));
  for (const item of items) json(item);
  return value as Json;
}

function attachAcceptance(value: Record<string, unknown>, sessionId: string, id: string): Acceptance {
  const accepted = { session_id: text(value.session_id), receipt: receipt(value.receipt), saving: json(value.saving) };
  if (accepted.session_id !== sessionId || accepted.receipt.request_id !== id || accepted.receipt.command !== "attach") {
    throw new CloudroomError("Cloudroom acceptance does not match the command");
  }
  return accepted;
}

function receipt(value: unknown): Receipt {
  const item = object(value);
  return {
    request_id: text(item.request_id),
    command: text(item.command),
    state: text(item.state),
    input: json(item.input),
    ...(item.model === undefined ? {} : { model: text(item.model) }),
    ...(item.provider === undefined ? {} : { provider: text(item.provider) }),
  };
}

function record(value: unknown, sessionId: string): SessionRecord {
  const item = object(value);
  if (item.session_id !== sessionId)
    throw new CloudroomError("Cloudroom session mismatch");
  return {
    sequence: sequence(item.sequence),
    session_id: sessionId,
    kind: text(item.kind),
    data: json(item.data),
    ...(item.native === undefined ? {} : { native: text(item.native) }),
    ...(item.timestamp_ms === undefined
      ? {}
      : { timestamp_ms: sequence(item.timestamp_ms) }),
  };
}

function requestId(value: string): string {
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(value)) {
    throw new CloudroomError(
      "request_id must be 1–64 letters, digits, underscores or hyphens",
    );
  }
  return value;
}

function sessionPath(id: string): string {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id))
    throw new CloudroomError("A valid Cloudroom session ID is required");
  return `/v1/sessions/${encodeURIComponent(id)}`;
}

const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
const STREAM_SILENCE_MS = 45_000;
/** The most one upload request may carry: some sandbox proxies drop requests over about 8 MB. */
export const UPLOAD_PART = 4 * 1024 * 1024;

export class CloudroomClient {
  #base: string;
  #token: string;
  #gateToken?: string;
  #timeoutMs: number;

  constructor(options: { url: string; token: string; gateToken?: string; timeoutMs?: number }) {
    let url: URL;
    try {
      url = new URL(options.url);
    } catch {
      throw new CloudroomError("Invalid Cloudroom URL");
    }
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
    if (
      (url.protocol !== "https:" && !(url.protocol === "http:" && local)) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    ) {
      throw new CloudroomError(
        "Use an HTTPS core URL without credentials, query or fragment",
      );
    }
    if (!options.token || /[^\x21-\x7e]/.test(options.token)) {
      throw new CloudroomError("A core access token is required");
    }
    if (options.gateToken !== undefined && !/^[a-zA-Z0-9._~-]{1,4096}$/.test(options.gateToken)) {
      throw new CloudroomError("Invalid gate credential");
    }
    this.#base = url.href.replace(/\/$/, "");
    this.#token = options.token;
    this.#gateToken = options.gateToken;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isSafeInteger(this.#timeoutMs) || this.#timeoutMs <= 0) {
      throw new CloudroomError("timeoutMs must be a positive integer");
    }
  }

  async prepareTeleport(manifest: TeleportManifest): Promise<TeleportStatus> {
    return await this.#json("/v1/teleports", manifest) as unknown as TeleportStatus;
  }
  /** Ask the VM to run the same Claude Code version as this Mac (ADR 0133). It switches only while no Claude session runs. */
  async matchClaudeVersion(version: string): Promise<{ state: "current" | "updating" | "waiting" | "failed"; installed?: string; target: string; error?: string | null }> {
    return await this.#json("/v1/accounts/claude/version", { version }) as unknown as { state: "current" | "updating" | "waiting" | "failed"; installed?: string; target: string; error?: string | null };
  }
  async checkTeleport(check: TeleportCheck, signal?: AbortSignal): Promise<TeleportCheckResult> {
    // One real model request runs on the VM, so allow longer than ordinary commands.
    return await this.#json("/v1/teleports/check", check, signal, undefined, 120_000) as unknown as TeleportCheckResult;
  }
  async teleportStatus(id: string): Promise<TeleportStatus> {
    return await this.#json(`/v1/teleports/${encodeURIComponent(id)}`) as unknown as TeleportStatus;
  }
  async activateTeleport(id: string, retryRequestId?: string): Promise<TeleportStatus> {
    return await this.#json(`/v1/teleports/${encodeURIComponent(id)}/activate`, retryRequestId ? { retry_request_id: retryRequestId } : {}) as unknown as TeleportStatus;
  }
  async cancelTeleport(id: string): Promise<TeleportStatus> {
    return await this.#json(`/v1/teleports/${encodeURIComponent(id)}/cancel`, {}) as unknown as TeleportStatus;
  }
  async uploadTeleport(id: string, index: number, offset: number, sha256: string, bytes: Uint8Array, size?: number): Promise<TeleportStatus> {
    return await this.#json(`/v1/teleports/${encodeURIComponent(id)}/files/${index}?offset=${offset}&sha256=${encodeURIComponent(sha256)}${size === undefined ? "" : `&size=${size}`}`, undefined, undefined, {
      body: new ReadableStream({ start(controller) { controller.enqueue(bytes); controller.close(); } }), length: bytes.length, contentType: "application/octet-stream",
    }) as unknown as TeleportStatus;
  }

  /** Runs a shell command on the VM as its agent account (ADR 0113). No timeout: the caller decides.
   *  Raw `bytes` replace hex stdin, halving the upload; older cores answer 404. */
  async runOnVm(input: VmRun, signal?: AbortSignal, bytes?: Uint8Array<ArrayBuffer>): Promise<VmRunResult> {
    const raw = bytes ? `/raw?${new URLSearchParams({ command: input.command, ...(input.cwd ? { cwd: input.cwd } : {}) })}` : "";
    let response: Response;
    try {
      response = await fetch(`${this.#base}/v1/vm/run${raw}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.#token}`,
          // Some sandbox proxies replace Authorization with their own login.
          "X-Cloudroom-Token": this.#token,
          ...(this.#gateToken ? { Cookie: `_port_auth=${this.#gateToken}` } : {}),
          Accept: "application/json",
          "Content-Type": bytes ? "application/octet-stream" : "application/json",
        },
        body: bytes ?? JSON.stringify(input),
        redirect: "error",
        signal,
      });
    } catch (error) {
      throw new CloudroomConnectionError(`Cloudroom is unreachable: ${describe(error)}`, error);
    }
    const value = await response.json().catch(() => null) as Record<string, unknown> | null;
    if (!response.ok || !value) {
      throw new CloudroomError(typeof value?.error === "string" ? value.error : `Cloudroom rejected the request (HTTP ${response.status})`, response.status);
    }
    return value as unknown as VmRunResult;
  }

  async #request(
    path: string,
    body?: Json,
    signal?: AbortSignal,
    upload?: Upload,
  ): Promise<Response> {
    let response: Response;
    try {
      response = await fetch(`${this.#base}${path}`, {
        method: body === undefined && !upload ? "GET" : "POST",
        headers: {
          Authorization: `Bearer ${this.#token}`,
          // Some sandbox proxies replace Authorization with their own login.
          "X-Cloudroom-Token": this.#token,
          ...(this.#gateToken ? { Cookie: `_port_auth=${this.#gateToken}` } : {}),
          Accept: path.includes("/stream?")
            ? "text/event-stream"
            : "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
          ...(upload ? { "Content-Type": upload.contentType ?? "application/gzip", "Content-Length": String(upload.length) } : {}),
        },
        ...(upload ? { body: upload.body, duplex: "half" } : body === undefined ? {} : { body: JSON.stringify(body) }),
        redirect: "error",
        signal,
      });
    } catch (error) {
      if (signal?.aborted)
        throw new DOMException(
          "Cloudroom request cancelled or timed out",
          "AbortError",
        );
      throw new CloudroomConnectionError(
        `Cloudroom is unreachable (${describe(error)}). For an unconfirmed command, retry the same request_id.`,
        error,
      );
    }
    if (!response.ok) {
      // Auth failures may echo credentials, so their body is never read.
      if (response.status === 401 || response.status === 403) {
        await response.body?.cancel();
        throw new CloudroomError(`Cloudroom authentication failed (HTTP ${response.status})`, response.status, null);
      }
      const { code: known, error } = await rejection(response);
      await response.body?.cancel().catch(() => {});
      // Only 400/409 codes steer retry decisions; other statuses keep status-based rules.
      const code = response.status === 400 || response.status === 409 ? known : null;
      const message = code ? rejectionMessages[code] : `Cloudroom rejected the request (HTTP ${response.status})${error ? `: ${error}` : ""}`;
      throw new CloudroomError(message, response.status, code);
    }
    return response;
  }

  async #json(
    path: string,
    body?: Json,
    signal?: AbortSignal,
    upload?: Upload,
    timeoutMs = this.#timeoutMs,
  ): Promise<Record<string, unknown>> {
    const timeout = AbortSignal.timeout(timeoutMs);
    const response = await this.#request(
      path,
      body,
      signal ? AbortSignal.any([signal, timeout]) : timeout,
      upload,
    );
    if ((body !== undefined || upload) && response.status !== 202) {
      await response.body?.cancel();
      throw new CloudroomError(
        "Cloudroom did not acknowledge command acceptance",
      );
    }
    if (!response.headers.get("content-type")?.includes("application/json")) {
      await response.body?.cancel();
      throw new CloudroomError("Expected a Cloudroom JSON response");
    }
    const reader = response.body?.getReader();
    if (!reader) throw new CloudroomError("Empty Cloudroom response");
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read().catch((error: unknown) => {
          throw new CloudroomConnectionError("Cloudroom response disconnected. Retry unconfirmed commands with the same request_id.", error);
        });
        if (done) break;
        length += value.byteLength;
        if (length > MAX_RESPONSE_BYTES)
          throw new CloudroomError("Cloudroom response exceeds the size limit");
        chunks.push(value);
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }
      return object(JSON.parse(new TextDecoder().decode(bytes)));
    } catch (error) {
      if (error instanceof CloudroomError) throw error;
      throw new CloudroomError(
        "Incomplete or invalid Cloudroom response. Retry unconfirmed commands with the same request_id.",
      );
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }

  async #command(
    path: string,
    command: string,
    body: { request_id: string } & Record<string, Json>,
    sessionId?: string,
  ): Promise<Acceptance> {
    requestId(body.request_id);
    const value = await this.#json(path, body);
    const accepted = {
      session_id: text(value.session_id),
      receipt: receipt(value.receipt),
      saving: json(value.saving),
    };
    if (
      (sessionId !== undefined && accepted.session_id !== sessionId) ||
      accepted.receipt.request_id !== body.request_id ||
      accepted.receipt.command !== command
    ) {
      throw new CloudroomError(
        "Cloudroom acceptance does not match the command",
      );
    }
    return accepted;
  }

  health(signal?: AbortSignal) {
    return this.#json("/v1/health", undefined, signal);
  }
  ready(signal?: AbortSignal) {
    return this.#json("/v1/ready", undefined, signal);
  }
  dashboard(signal?: AbortSignal) {
    return this.#json("/v1/dashboard", undefined, signal);
  }
  /** The latest 1,000 sessions, newest activity first. */
  async listSessions(signal?: AbortSignal): Promise<{ total: number; sessions: SessionSummary[] }> {
    const value = await this.#json("/v1/sessions", undefined, signal);
    if (!Array.isArray(value.sessions)) throw new CloudroomError("Invalid Cloudroom session list");
    return { total: sequence(value.total), sessions: value.sessions.map(item => object(item) as SessionSummary) };
  }
  async codexAuth(action?: "login" | "cancel", id?: string, signal?: AbortSignal): Promise<CodexAuthStatus> {
    const value = await this.#json(`/v1/accounts/codex${action ? `/${action}` : ""}`, action ? { request_id: requestId(id ?? "") } : undefined, signal);
    if (!["missing", "waiting", "connected", "limited", "unavailable", "error", "expired"].includes(String(value.state))) throw new CloudroomError("Invalid Codex account status");
    const nullable = (key: string) => value[key] === null ? null : text(value[key]);
    const result = { state: value.state as CodexAuthStatus["state"], email: nullable("email"), plan: nullable("plan"), message: nullable("message"), login_id: nullable("login_id"), verification_url: nullable("verification_url"), user_code: nullable("user_code") };
    if (result.verification_url !== null && result.verification_url !== "https://auth.openai.com/codex/device") throw new CloudroomError("Unexpected Codex sign-in URL");
    return result;
  }

  async claudeAuth(action?: "login" | "cancel" | "complete" | "token" | "key", id?: string, code?: string, state?: string, signal?: AbortSignal): Promise<CodexAuthStatus> {
    const value = await this.#json(`/v1/accounts/claude${action ? `/${action}` : ""}`, action ? { request_id: requestId(id ?? ""), ...(action === "complete" ? { code, state } : {}), ...(action === "token" ? { token: code, ...(state ? { plan: state } : {}) } : {}), ...(action === "key" ? { api_key: code } : {}) } : undefined, signal, undefined, action === "key" ? 60_000 : undefined);
    if (!["missing", "waiting", "connected", "unavailable", "error", "expired"].includes(String(value.state))) throw new CloudroomError("Invalid Claude account status");
    const nullable = (key: string) => value[key] === null ? null : text(value[key]);
    const result = { state: value.state as CodexAuthStatus["state"], email: nullable("email"), plan: nullable("plan"), message: nullable("message"), login_id: nullable("login_id"), verification_url: nullable("verification_url"), user_code: nullable("user_code") };
    if (result.verification_url !== null) {
      const url = new URL(result.verification_url);
      if (url.protocol !== "https:" || url.username || url.password || !["claude.ai", "claude.com", "platform.claude.com"].includes(url.hostname) || !["/oauth/authorize", "/cai/oauth/authorize"].includes(url.pathname)) throw new CloudroomError("Unexpected Claude sign-in URL");
    }
    return result;
  }

  /** Saves one Pi provider key on the VM. Returns provider names only, never secrets. */
  async piApiKey(provider: string, key: string, signal?: AbortSignal): Promise<{ providers: string[] }> {
    const value = await this.#json("/v1/accounts/pi/key", { provider, key }, signal);
    if (!Array.isArray(value.providers) || !value.providers.every(name => typeof name === "string")) throw new CloudroomError("Invalid Pi account status");
    return { providers: value.providers };
  }

  async cursorAuth(action?: "login" | "cancel" | "key", id?: string, apiKey?: string, signal?: AbortSignal): Promise<CodexAuthStatus> {
    const value = await this.#json(`/v1/accounts/cursor${action ? `/${action}` : ""}`, action ? { request_id: requestId(id ?? ""), ...(action === "key" ? { api_key: apiKey } : {}) } : undefined, signal);
    if (!["missing", "waiting", "connected", "limited", "unavailable", "error", "expired"].includes(String(value.state))) throw new CloudroomError("Invalid Cursor account status");
    const nullable = (key: string) => value[key] === null ? null : text(value[key]);
    const result = { state: value.state as CodexAuthStatus["state"], email: nullable("email"), plan: nullable("plan"), message: nullable("message"), login_id: nullable("login_id"), verification_url: nullable("verification_url"), user_code: nullable("user_code") };
    if (result.verification_url !== null) {
      const url = new URL(result.verification_url);
      if (url.origin !== "https://cursor.com" || url.pathname !== "/loginDeepControl" || url.username || url.password) throw new CloudroomError("Unexpected Cursor sign-in URL");
    }
    return result;
  }

  capabilities(signal?: AbortSignal) {
    return this.#json("/v1/capabilities", undefined, signal);
  }

  async workspace(id: string, signal?: AbortSignal): Promise<Workspace | null> {
    requestId(id);
    try {
      const value = await this.#json(`/v1/workspaces/${id}`, undefined, signal);
      if (value.id !== id) throw new CloudroomError("Cloudroom workspace mismatch");
      return { id, path: text(value.path), ...(value.parent === undefined ? {} : { parent: text(value.parent) }) };
    } catch (error) {
      if (error instanceof CloudroomError && error.status === 404) return null;
      throw error;
    }
  }

  start(id: string, harness: Harness = "codex", options: { model?: string; reasoning?: string; workspace?: string; workspace_name?: string; provider?: string; command_guard_enabled?: boolean; strip_ai_co_authors?: boolean; system_prompt?: string; parent_session?: string; prompt?: string; title?: string; fork?: { session: string; before?: string; last_turn_id?: string } } = {}) {
    if (harness !== "codex" && harness !== "pi" && harness !== "cursor" && harness !== "claude-code" && harness !== "fx" && harness !== "opencode")
      throw new CloudroomError("Unsupported Cloudroom harness");
    return this.#command("/v1/sessions", "start", { request_id: id, harness, ...options });
  }

  prompt(
    sessionId: string,
    id: string,
    prompt: string,
    reasoning?: string,
    extra: { content?: Json; attachments?: Json; service_tier?: string } = {},
  ) {
    if ((!prompt.trim() && extra.attachments === undefined) || new TextEncoder().encode(prompt).length > 32768) {
      throw new CloudroomError("Prompt must contain 1–32768 bytes of text", 400);
    }
    return this.#command(
      `${sessionPath(sessionId)}/prompts`,
      "prompt",
      {
        request_id: id,
        text: prompt,
        ...(reasoning ? { reasoning } : {}),
        ...(extra.content === undefined ? {} : { content: extra.content }),
        ...(extra.attachments === undefined ? {} : { attachments: extra.attachments }),
        ...(extra.service_tier ? { service_tier: extra.service_tier } : {}),
      },
      sessionId,
    );
  }

  edit(
    sessionId: string,
    id: string,
    targetRequestId: string,
    expectedRevision: number,
    prompt: string,
    extra: { content?: Json; attachments?: Json; reasoning?: string; service_tier?: string } = {},
  ) {
    return this.#command(
      `${sessionPath(sessionId)}/edit`,
      "edit",
      {
        request_id: id,
        target_request_id: requestId(targetRequestId),
        expected_revision: expectedRevision,
        text: prompt,
        ...extra,
      },
      sessionId,
    );
  }

  cancel(sessionId: string, id: string, targetRequestId: string) {
    return this.#command(
      `${sessionPath(sessionId)}/cancel`,
      "cancel",
      { request_id: id, target_request_id: requestId(targetRequestId) },
      sessionId,
    );
  }

  reorder(sessionId: string, id: string, order: string[]) {
    return this.#command(
      `${sessionPath(sessionId)}/reorder`,
      "reorder",
      { request_id: id, order: order.map(requestId) },
      sessionId,
    );
  }

  steer(sessionId: string, id: string, targetRequestId: string, text: string) {
    if (!text.trim() || new TextEncoder().encode(text).length > 32768) {
      throw new CloudroomError("Prompt must contain 1–32768 bytes of text", 400);
    }
    return this.#command(
      `${sessionPath(sessionId)}/steer`,
      "steer",
      { request_id: id, target_request_id: requestId(targetRequestId), text },
      sessionId,
    );
  }

  compact(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/compact`, "compact", { request_id: id }, sessionId);
  }

  /** The user's goal control: pause or resume (`status`), replace the `objective`, or `clear`. */
  goal(sessionId: string, id: string, goal: { status?: "active" | "paused"; objective?: string; clear?: boolean }) {
    return this.#command(`${sessionPath(sessionId)}/goal`, "goal", { request_id: id, ...goal }, sessionId);
  }

  /** `fork` also gives a whole-conversation copy its own native identity, with no checkpoint. */
  rewind(sessionId: string, id: string, before?: string, lastTurnId?: string, replacement?: { request_id: string; text: string; content?: Json; attachments?: Json; reasoning?: string; service_tier?: string }, fork = false) {
    return this.#command(
      `${sessionPath(sessionId)}/rewind`,
      "rewind",
      {
        request_id: id,
        ...(before ? { before } : {}),
        ...(lastTurnId ? { last_turn_id: lastTurnId } : {}),
        ...(replacement ? { replacement } : {}),
        ...(fork ? { fork: true } : {}),
      },
      sessionId,
    );
  }

  async attach(
    sessionId: string,
    id: string,
    name: string,
    kind: "image" | "file",
    upload: Upload,
    signal?: AbortSignal,
  ): Promise<Acceptance> {
    requestId(id);
    if (!Number.isSafeInteger(upload.length) || upload.length <= 0 || upload.length > 25 * 1024 * 1024) {
      throw new CloudroomError("Attachment exceeds the size limit");
    }
    const query = new URLSearchParams({ request_id: id, name, kind });
    const value = await this.#json(
      `${sessionPath(sessionId)}/attachments?${query}`,
      undefined,
      signal,
      { ...upload, contentType: "application/octet-stream" },
    );
    return attachAcceptance(value, sessionId, id);
  }

  /** Sends an attachment in `UPLOAD_PART` pieces (core `upload_parts`); the last part returns the acceptance.
   *  A retry resends every part: the core ignores parts it already has. */
  async attachInParts(
    sessionId: string,
    id: string,
    name: string,
    kind: "image" | "file",
    bytes: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Acceptance> {
    requestId(id);
    if (bytes.length > 25 * 1024 * 1024) throw new CloudroomError("Attachment exceeds the size limit");
    let offset = 0;
    do {
      const part = bytes.subarray(offset, offset + UPLOAD_PART);
      const query = new URLSearchParams({ request_id: id, name, kind, offset: String(offset), total: String(bytes.length) });
      const value = await this.#json(`${sessionPath(sessionId)}/attachments?${query}`, undefined, signal, {
        body: new ReadableStream({ start(controller) { controller.enqueue(part); controller.close(); } }), length: part.length, contentType: "application/octet-stream",
      });
      if (value.receipt !== undefined) return attachAcceptance(value, sessionId, id);
      offset += part.length;
    } while (offset < bytes.length);
    throw new CloudroomError("Cloudroom did not save the attachment");
  }

  interrupt(sessionId: string, id: string, targetRequestId: string) {
    return this.#command(
      `${sessionPath(sessionId)}/interrupt`,
      "interrupt",
      { request_id: id, target_request_id: requestId(targetRequestId) },
      sessionId,
    );
  }

  stop(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/stop`, "stop", { request_id: id }, sessionId);
  }

  resume(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/resume`, "resume", { request_id: id }, sessionId);
  }

  sleep(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/sleep`, "sleep", { request_id: id }, sessionId);
  }

  /** Answers an agent's secret request. `null` cancels it. */
  async answerSecret(sessionId: string, id: string, values: Record<string, string> | null): Promise<void> {
    await this.#json(`${sessionPath(sessionId)}/secrets/${encodeURIComponent(id)}`, values ? { values } : {});
  }

  close(sessionId: string, id: string) {
    return this.#command(
      `${sessionPath(sessionId)}/close`,
      "close",
      { request_id: id },
      sessionId,
    );
  }

  async sessionWorkspace(sessionId: string, signal?: AbortSignal): Promise<{ path: string; branch: string | null; head: string | null }> {
    const value = await this.#json(`${sessionPath(sessionId)}/workspace`, undefined, signal);
    return {
      path: text(value.path),
      branch: value.branch === null ? null : text(value.branch),
      head: value.head === null ? null : text(value.head),
    };
  }

  async session(sessionId: string, signal?: AbortSignal): Promise<Session> {
    const value = object(
      (await this.#json(sessionPath(sessionId), undefined, signal)).session,
    );
    if (
      value.session_id !== sessionId ||
      (value.harness !== "codex" && value.harness !== "pi" && value.harness !== "cursor" && value.harness !== "claude-code" && value.harness !== "fx" && value.harness !== "opencode")
    ) {
      throw new CloudroomError(
        "Cloudroom session mismatch or unsupported harness",
      );
    }
    if (!Array.isArray(value.queue))
      throw new CloudroomError("Invalid Cloudroom queue");
    return {
      session_id: sessionId,
      harness: value.harness,
      state: text(value.state),
      native_id: value.native_id === null ? null : text(value.native_id),
      current_request:
        value.current_request === null ? null : text(value.current_request),
      last_sequence: sequence(value.last_sequence),
      queue: value.queue.map(text),
      receipts: Object.fromEntries(
        Object.entries(object(value.receipts)).map(([id, item]) => [
          id,
          receipt(item),
        ]),
      ),
    };
  }

  async events(
    sessionId: string,
    after = 0,
    signal?: AbortSignal,
  ): Promise<SessionRecord[]> {
    let cursor = sequence(after);
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    const replaySignal = signal ? AbortSignal.any([signal, timeout]) : timeout;
    const lastSequence = (await this.session(sessionId, replaySignal))
      .last_sequence;
    if (cursor >= lastSequence) return [];
    const records: SessionRecord[] = [];
    for await (const item of this.#stream(sessionId, {
      after,
      signal: replaySignal,
    })) {
      if (item.sequence <= cursor)
        throw new CloudroomError("Cloudroom history is out of order");
      if (item.sequence > lastSequence) break;
      records.push(item);
      cursor = item.sequence;
      if (cursor === lastSequence) return records;
    }
    throw new CloudroomError("Incomplete Cloudroom history");
  }

  async *stream(
    sessionId: string,
    options: { after?: number; signal: AbortSignal; onConnected?: () => void | Promise<void> },
  ): AsyncGenerator<SessionRecord> {
    let cursor = sequence(options.after ?? 0);
    for await (const item of this.#stream(sessionId, options)) {
      if (item.sequence > cursor) {
        cursor = item.sequence;
        yield item;
      }
    }
  }

  async *#stream(
    sessionId: string,
    options: { after?: number; signal: AbortSignal; onConnected?: () => void | Promise<void> },
  ): AsyncGenerator<SessionRecord> {
    const cursor = sequence(options.after ?? 0);
    const response = await this.#request(
      `${sessionPath(sessionId)}/stream?after=${cursor}`,
      undefined,
      options.signal,
    );
    if (!response.headers.get("content-type")?.includes("text/event-stream")) {
      await response.body?.cancel();
      throw new CloudroomError("Expected a Cloudroom event stream");
    }
    const reader = response.body?.getReader();
    if (!reader) throw new CloudroomError("Empty Cloudroom stream");
    const decoder = new TextDecoder();
    let buffer = "";
    let data: string[] = [];
    let event = "";
    let id = "";
    let frameSize = 0;
    try {
      options.signal.throwIfAborted();
      await options.onConnected?.();
      while (true) {
        let silence: ReturnType<typeof setTimeout> | undefined;
        const { done, value } = await Promise.race([
          reader.read(),
          new Promise<never>((_, reject) => { silence = setTimeout(() => reject(new Error("no data for 45 s")), STREAM_SILENCE_MS); }),
        ]).catch((error: unknown) => {
          options.signal.throwIfAborted();
          throw new CloudroomConnectionError("Cloudroom event stream disconnected", error);
        }).finally(() => clearTimeout(silence));
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        if (buffer.length + frameSize > MAX_RESPONSE_BYTES)
          throw new CloudroomError("Cloudroom event exceeds the size limit");
        let newline: number;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline).replace(/\r$/, "");
          buffer = buffer.slice(newline + 1);
          if (line === "") {
            if (event === "record" && data.length > 0) {
              let parsed: unknown;
              try {
                parsed = JSON.parse(data.join("\n"));
              } catch {
                throw new CloudroomError("Invalid Cloudroom event JSON");
              }
              const item = record(parsed, sessionId);
              if (id !== String(item.sequence))
                throw new CloudroomError("Cloudroom event cursor mismatch");
              yield item;
            }
            data = [];
            event = "";
            id = "";
            frameSize = 0;
          } else if (!line.startsWith(":")) {
            frameSize += line.length;
            if (frameSize > MAX_RESPONSE_BYTES)
              throw new CloudroomError(
                "Cloudroom event exceeds the size limit",
              );
            const separator = line.indexOf(":");
            const field = separator === -1 ? line : line.slice(0, separator);
            const content =
              separator === -1
                ? ""
                : line.slice(separator + 1).replace(/^ /, "");
            if (field === "data") data.push(content);
            if (field === "event") event = content;
            if (field === "id") id = content;
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
}
