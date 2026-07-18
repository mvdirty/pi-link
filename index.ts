/**
 * Pi Link — WebSocket-based inter-terminal communication
 *
 * Connects multiple Pi terminals over a local WebSocket link.
 * Opt-in via --link flag, --link-name flag, pi-link CLI, or /link-connect command.
 * First terminal to connect becomes the hub; others join as clients.
 * Hub loss triggers automatic promotion of a surviving client.
 *
 * Tools: link_send, link_prompt, link_list, link_compact
 * Commands: /link, /link-name, /link-broadcast, /link-connect, /link-disconnect
 */

import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { Type } from "typebox";
import * as crypto from "node:crypto";
import * as os from "node:os";

import { WebSocket, WebSocketServer } from "ws";

// ─── Constants ───────────────────────────────────────────────────────────────

const DEFAULT_PORT = 9900;
const PROMPT_INACTIVITY_MS = 90_000;
const PROMPT_HARD_CEILING_MS = 1_800_000;
const COMPACT_TIMEOUT_MS = 180_000;
const RECONNECT_DELAY_MS = 2000;
const KEEPALIVE_INTERVAL_MS = 30_000;
const FLUSH_DELAY_MS = 200;
const IDLE_RETRY_MS = 500;
const BATCH_MAX_ITEMS = 20;
const BATCH_MAX_CHARS = 16_000;

// ─── Protocol ────────────────────────────────────────────────────────────────

interface RegisterMsg {
  type: "register";
  name: string;
  cwd?: string;
  context?: ContextSnapshot;
}
interface WelcomeMsg {
  type: "welcome";
  name: string;
  terminals: string[];
  statuses?: Record<string, LinkStatus>;
  cwds?: Record<string, string>;
  contexts?: Record<string, ContextSnapshot>;
}
interface TerminalJoinedMsg {
  type: "terminal_joined";
  name: string;
  terminals: string[];
  cwd?: string;
  context?: ContextSnapshot;
}
interface TerminalLeftMsg {
  type: "terminal_left";
  name: string;
  terminals: string[];
}
interface ChatMsg {
  type: "chat";
  from: string;
  to: string;
  content: string;
  triggerTurn: boolean;
}
interface PromptRequestMsg {
  type: "prompt_request";
  id: string;
  from: string;
  to: string;
  prompt: string;
}
interface PromptResponseMsg {
  type: "prompt_response";
  id: string;
  from: string;
  to: string;
  response: string;
  error?: string;
}
interface StatusUpdateMsg {
  type: "status_update";
  name: string;
  status: LinkStatus;
  // Per-terminal LLM context. Absent = old terminal (ignore); null = clear
  // stored value; object = store. Only status_update carries the null-clear.
  context?: ContextSnapshot | null;
}
interface ErrorMsg {
  type: "error";
  message: string;
}
interface CompactRequestMsg {
  type: "compact_request";
  id: string;
  from: string;
  to: string;
  instructions?: string;
}
interface CompactResponseMsg {
  type: "compact_response";
  id: string;
  from: string;
  to: string;
  ok: boolean;
  reason?: string; // "busy" | "not_found" | "unsupported" | error text; absent on success
}

type LinkStatus =
  | { kind: "idle"; since: number }
  | { kind: "thinking"; since: number }
  | { kind: "tool"; toolName: string; since: number };

type ContextSnapshot = { tokens: number | null; contextWindow: number };

type LinkMessage =
  | RegisterMsg
  | WelcomeMsg
  | TerminalJoinedMsg
  | TerminalLeftMsg
  | ChatMsg
  | PromptRequestMsg
  | PromptResponseMsg
  | StatusUpdateMsg
  | ErrorMsg
  | CompactRequestMsg
  | CompactResponseMsg;

// ─── Extension ───────────────────────────────────────────────────────────────

export default function (pi: ExtensionAPI) {
  pi.registerFlag("link", {
    description: "Connect to link on startup",
    type: "boolean",
    default: false,
  });

  pi.registerFlag("link-name", {
    description:
      "Set the pi-link terminal name on startup (link identity only; does not affect session)",
    type: "string",
  });

  // ── State ────────────────────────────────────────────────────────────────

  let role: "hub" | "client" | "disconnected" = "disconnected";
  let terminalName = `t-${crypto.randomUUID().slice(0, 4)}`;
  let preferredName: string | null = null;
  // True between a client `/link-name` close and the next welcome/promotion.
  // Lets `startHub` adopt the requested name if it wins promotion before welcome.
  let pendingClientRename = false;
  let connectedTerminals: string[] = [];
  let ctx: ExtensionContext | undefined;
  let disposed = false;
  let manuallyDisconnected = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let startupConnectTimer: ReturnType<typeof setTimeout> | null = null;

  // Status tracking (local truth)
  let agentRunning = false;
  let compactRunning = false; // true while compacting for a remote request
  let activeToolName: string | null = null;
  let stateSince = Date.now();
  let lastPushedKind: string | null = null;
  let lastPushedTool: string | null = null;
  const terminalStatuses = new Map<string, LinkStatus>(); // other terminals
  const terminalContexts = new Map<string, ContextSnapshot>(); // other terminals' context
  let currentCwd = "";
  const terminalCwds = new Map<string, string>(); // other terminals' cwds

  // Hub state
  let wss: WebSocketServer | null = null;
  const hubClients = new Map<WebSocket, string>(); // ws → terminal name
  const hubTerminalStatuses = new Map<string, LinkStatus>(); // hub-authoritative
  const hubTerminalContexts = new Map<string, ContextSnapshot>(); // hub-authoritative
  const hubTerminalCwds = new Map<string, string>(); // hub-authoritative (excludes self)

  // Client state
  let ws: WebSocket | null = null;

  // Pending prompt responses (sender waiting for remote answer)
  const pendingPromptResponses = new Map<
    string,
    {
      resolve: (result: {
        content: { type: "text"; text: string }[];
        details: Record<string, unknown>;
      }) => void;
      targetName: string;
      inactivityTimeout: ReturnType<typeof setTimeout>;
      ceilingTimeout: ReturnType<typeof setTimeout>;
    }
  >();

  // Pending compact responses (sender waiting for remote compaction to finish)
  const pendingCompactResponses = new Map<
    string,
    {
      resolve: (result: {
        content: { type: "text"; text: string }[];
        details: Record<string, unknown>;
      }) => void;
      targetName: string;
      timeout: ReturnType<typeof setTimeout>;
    }
  >();

  // Pending remote prompt (this terminal is executing a prompt for someone else)
  let pendingRemotePrompt: { id: string; from: string } | null = null;
  let keepaliveTimer: ReturnType<typeof setInterval> | null = null;

  // Inbox: idle-gated batched delivery for triggerTurn:true messages
  const inbox: { from: string; content: string }[] = [];
  let flushTimer: ReturnType<typeof setTimeout> | null = null;

  // ── Helpers ──────────────────────────────────────────────────────────────

  function getUi() {
    if (!ctx) return null;
    try {
      return ctx.ui;
    } catch {
      return null;
    }
  }

  function isRuntimeLive() {
    return !disposed && getUi() !== null;
  }

  function notify(message: string, level: "info" | "warning" | "error") {
    getUi()?.notify(message, level);
  }

  function updateStatus() {
    const ui = getUi();
    if (!ui) return;
    const theme = ui.theme;
    const count = connectedTerminals.length;
    const info =
      role === "disconnected"
        ? "link: offline"
        : `link: ${terminalName} (${role}) · ${count} terminal${count !== 1 ? "s" : ""}`;
    ui.setStatus("link", theme.fg("dim", info));
  }

  function deriveStatus(): LinkStatus {
    if (activeToolName)
      return { kind: "tool", toolName: activeToolName, since: stateSince };
    if (agentRunning) return { kind: "thinking", since: stateSince };
    return { kind: "idle", since: stateSince };
  }

  function captureContext(): ContextSnapshot | undefined {
    if (!ctx) return undefined;
    if (typeof ctx.getContextUsage !== "function") return undefined; // older Pi
    const usage = ctx.getContextUsage();
    if (!usage) return undefined;
    if (usage.contextWindow <= 0) return undefined; // no real context to report
    return { tokens: usage.tokens, contextWindow: usage.contextWindow };
  }

  function pushStatus(force = false) {
    if (role === "disconnected") return;
    const status = deriveStatus();
    const newKind = status.kind;
    const newTool = status.kind === "tool" ? status.toolName : null;
    if (!force && newKind === lastPushedKind && newTool === lastPushedTool)
      return;
    lastPushedKind = newKind;
    lastPushedTool = newTool;
    const context = captureContext(); // only when we actually push
    const msg: StatusUpdateMsg = {
      type: "status_update",
      name: terminalName,
      status,
      context: context ?? null, // explicit null tells peers to clear
    };
    if (role === "hub") {
      hubBroadcast(msg, terminalName);
    } else if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }

  // Canonicalize a link/session name: trim + collapse internal whitespace.
  // Returns undefined for nullish/blank so callers can fall through precedence.
  function normalizeName(name: string | undefined | null): string | undefined {
    const n = name?.trim().replace(/\s+/g, " ");
    return n ? n : undefined;
  }

  // Latest custom session entry of a given type (last-write-wins), or undefined.
  function latestCustomData(
    customType: string,
  ): Record<string, unknown> | undefined {
    if (!ctx) return undefined;
    const entries = ctx.sessionManager.getEntries();
    for (let i = entries.length - 1; i >= 0; i--) {
      const e = entries[i] as {
        type: string;
        customType?: string;
        data?: Record<string, unknown>;
      };
      if (e.type === "custom" && e.customType === customType) return e.data;
    }
    return undefined;
  }

  function formatDuration(since: number): string {
    const sec = Math.floor((Date.now() - since) / 1000);
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m`;
    return `${Math.floor(sec / 3600)}h`;
  }

  function formatStatus(s: LinkStatus): string {
    const dur = formatDuration(s.since);
    if (s.kind === "tool") return `tool:${s.toolName} (${dur})`;
    return `${s.kind} (${dur})`;
  }

  function formatTokens(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${Math.round(n / 1000)}K`;
    return `${n}`;
  }

  function formatContext(c: ContextSnapshot | null | undefined): string {
    if (!c || c.contextWindow <= 0) return ""; // guard against bad wire data
    const window = formatTokens(c.contextWindow);
    if (c.tokens === null) return `?/${window}`;
    const percent = Math.round((c.tokens / c.contextWindow) * 100);
    return `${formatTokens(c.tokens)}/${window} (${percent}%)`;
  }

  function getStatusFor(name: string): LinkStatus | null {
    if (name === terminalName) return deriveStatus();
    const map = role === "hub" ? hubTerminalStatuses : terminalStatuses;
    return map.get(name) ?? null;
  }

  function getCwdFor(name: string): string | null {
    if (name === terminalName) return currentCwd || null;
    if (role === "hub") return hubTerminalCwds.get(name) ?? null;
    return terminalCwds.get(name) ?? null;
  }

  function getContextFor(name: string): ContextSnapshot | null {
    if (name === terminalName) return captureContext() ?? null;
    if (role === "hub") return hubTerminalContexts.get(name) ?? null;
    return terminalContexts.get(name) ?? null;
  }

  function shortenPath(cwd: string): string {
    const home = os.homedir().replace(/\\/g, "/");
    const normalized = cwd.replace(/\\/g, "/");
    if (normalized === home) return "~";
    if (normalized.startsWith(home + "/"))
      return "~" + normalized.slice(home.length);
    return normalized;
  }

  // ── Startup connect ──────────────────────────────────────────────────────

  function scheduleStartupConnect() {
    if (startupConnectTimer) clearTimeout(startupConnectTimer);
    startupConnectTimer = setTimeout(() => {
      startupConnectTimer = null;
      if (!disposed && ctx) initialize();
    }, 0);
  }

  // ── Inbox: idle-gated batched delivery ───────────────────────────────────

  function scheduleFlush(delay: number) {
    if (flushTimer) clearTimeout(flushTimer);
    flushTimer = setTimeout(flushInbox, delay);
  }

  function flushInbox() {
    flushTimer = null;
    if (inbox.length === 0) return;
    if (!ctx) return;

    // Only deliver when idle so triggerTurn takes the prompt-start path
    // instead of mid-run steering, avoiding async delivery loss.
    let idle: boolean;
    try {
      idle = ctx.isIdle();
    } catch {
      return; // stale context — bail without retry
    }
    if (!idle) {
      scheduleFlush(IDLE_RETRY_MS);
      return;
    }

    // Select batch: up to BATCH_MAX_ITEMS, ~BATCH_MAX_CHARS total (soft cap —
    // first item always included even if oversized, others deferred to next flush)
    const batch: string[] = [];
    let totalChars = 0;
    for (let i = 0; i < inbox.length && batch.length < BATCH_MAX_ITEMS; i++) {
      const item = inbox[i];
      const text = `From "${item.from}":\n${item.content}`;
      if (batch.length > 0 && totalChars + text.length > BATCH_MAX_CHARS) break;
      batch.push(text);
      totalChars += text.length;
    }

    pi.sendMessage(
      {
        customType: "link",
        content: `[Link: ${batch.length} message(s) received]\n\n${batch.join("\n\n")}`,
        display: true,
        details: { batched: true, count: batch.length },
      },
      { triggerTurn: true },
    );
    inbox.splice(0, batch.length);

    // Reschedule if inbox still has items; agent_end wakeup will usually beat this
    if (inbox.length > 0) {
      scheduleFlush(IDLE_RETRY_MS);
    }
  }

  // ── Connection intent ──────────────────────────────────────────────────

  function shouldConnect(): boolean {
    const data = latestCustomData("link-active") as
      | { active?: boolean }
      | undefined;
    if (data?.active !== undefined) return data.active;
    return pi.getFlag("link") === true;
  }

  // ── Pending prompt helpers ───────────────────────────────────────────────

  function cleanupPending(requestId: string) {
    const pending = pendingPromptResponses.get(requestId);
    if (!pending) return null;
    clearTimeout(pending.inactivityTimeout);
    clearTimeout(pending.ceilingTimeout);
    pendingPromptResponses.delete(requestId);
    return pending;
  }

  function cleanupPendingCompact(requestId: string) {
    const pending = pendingCompactResponses.get(requestId);
    if (!pending) return null;
    clearTimeout(pending.timeout);
    pendingCompactResponses.delete(requestId);
    return pending;
  }

  function makeInactivityTimeout(requestId: string, targetName: string) {
    return setTimeout(() => {
      const pending = cleanupPending(requestId);
      if (pending) {
        pending.resolve(
          textResult(
            `Prompt to "${targetName}" timed out (no activity for ${PROMPT_INACTIVITY_MS / 1000}s)`,
            { to: targetName, error: "timeout" },
          ),
        );
      }
    }, PROMPT_INACTIVITY_MS);
  }

  function resetInactivityFor(targetName: string) {
    for (const [id, pending] of pendingPromptResponses) {
      if (pending.targetName === targetName) {
        clearTimeout(pending.inactivityTimeout);
        pending.inactivityTimeout = makeInactivityTimeout(id, targetName);
      }
    }
  }

  function allTerminalNames(): Set<string> {
    const names = new Set<string>();
    names.add(terminalName); // hub's own name
    for (const name of hubClients.values()) names.add(name);
    return names;
  }

  function uniqueName(requested: string): string {
    const existing = allTerminalNames();
    if (!existing.has(requested)) return requested;
    let i = 2;
    while (existing.has(`${requested}-${i}`)) i++;
    return `${requested}-${i}`;
  }

  function terminalList(): string[] {
    return Array.from(allTerminalNames()).sort();
  }

  function safeParse(data: string): LinkMessage | null {
    try {
      return JSON.parse(data);
    } catch {
      return null;
    }
  }

  // ── Routing ──────────────────────────────────────────────────────────────

  /** Hub: broadcast a message to every terminal except `excludeName`. */
  function hubBroadcast(msg: LinkMessage, excludeName?: string) {
    const json = JSON.stringify(msg);
    for (const [clientWs, name] of hubClients) {
      if (name !== excludeName) clientWs.send(json);
    }
    // Also deliver to the hub itself (unless excluded)
    if (excludeName !== terminalName) handleIncoming(msg);
  }

  /** Hub: find a client WebSocket by name. */
  function hubClientByName(name: string): WebSocket | undefined {
    for (const [clientWs, n] of hubClients) {
      if (n === name) return clientWs;
    }
    return undefined;
  }

  /**
   * Route a message to its destination. Works in both hub and client roles.
   * Returns true if the message was delivered (or sent to the hub for routing).
   * For the hub, this is authoritative. For clients, it's optimistic (hub may
   * still reject via protocol-level error responses).
   */
  function routeMessage(
    msg:
      | ChatMsg
      | PromptRequestMsg
      | PromptResponseMsg
      | CompactRequestMsg
      | CompactResponseMsg,
  ): boolean {
    if (role === "hub") {
      if (msg.to === "*") {
        hubBroadcast(msg, msg.from);
        return true;
      }
      if (msg.to === terminalName) {
        handleIncoming(msg);
        return true;
      }
      const targetWs = hubClientByName(msg.to);
      if (targetWs) {
        targetWs.send(JSON.stringify(msg));
        return true;
      }
      // Target not found — send error back to sender
      const errText = `Terminal "${msg.to}" not found`;
      const errorMsg: LinkMessage =
        msg.type === "prompt_request"
          ? {
              type: "prompt_response",
              id: msg.id,
              from: terminalName,
              to: msg.from,
              response: "",
              error: errText,
            }
          : msg.type === "compact_request"
            ? {
                type: "compact_response",
                id: msg.id,
                from: terminalName,
                to: msg.from,
                ok: false,
                reason: "not_found",
              }
            : { type: "error", message: errText };

      if (msg.from === terminalName) {
        // For prompt_request/compact_request, deliver the error response
        // locally so the matching pending map resolves. For chat, skip — the
        // tool result (via return false) is sufficient; no extra UI toast.
        if (
          errorMsg.type === "prompt_response" ||
          errorMsg.type === "compact_response"
        )
          handleIncoming(errorMsg);
      } else {
        hubClientByName(msg.from)?.send(JSON.stringify(errorMsg));
      }
      return false;
    }
    if (role === "client" && ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
      return true; // optimistic — hub will handle errors via protocol
    }
    return false;
  }

  // ── Incoming message handler (runs on every terminal) ────────────────────

  function handleIncoming(msg: LinkMessage) {
    switch (msg.type) {
      // ── Client receives after registering ──
      case "welcome":
        terminalName = msg.name;
        pendingClientRename = false;
        connectedTerminals = msg.terminals;
        terminalStatuses.clear();
        terminalCwds.clear();
        terminalContexts.clear();
        if (msg.statuses) {
          for (const [name, status] of Object.entries(msg.statuses)) {
            terminalStatuses.set(name, status);
          }
        }
        if (msg.cwds) {
          for (const [name, cwd] of Object.entries(msg.cwds)) {
            terminalCwds.set(name, cwd);
          }
        }
        if (msg.contexts) {
          for (const [name, c] of Object.entries(msg.contexts)) {
            terminalContexts.set(name, c);
          }
        }
        updateStatus();
        notify(
          `Joined link as "${terminalName}" (${connectedTerminals.length} online)`,
          "info",
        );
        pushStatus(true);
        break;

      // ── Membership updates ──
      case "terminal_joined":
        connectedTerminals = msg.terminals;
        if (role !== "hub" && msg.cwd) terminalCwds.set(msg.name, msg.cwd);
        if (role !== "hub" && msg.context)
          terminalContexts.set(msg.name, msg.context);
        updateStatus();
        notify(`"${msg.name}" joined the link`, "info");
        break;

      case "terminal_left":
        connectedTerminals = msg.terminals;
        terminalStatuses.delete(msg.name);
        if (role !== "hub") {
          terminalCwds.delete(msg.name);
          terminalContexts.delete(msg.name);
        }
        // Fail any pending prompts/compacts to the departed terminal
        for (const [id, pending] of pendingPromptResponses) {
          if (pending.targetName === msg.name) {
            const p = cleanupPending(id);
            if (p) {
              p.resolve(
                textResult(`Terminal "${msg.name}" disconnected`, {
                  to: msg.name,
                  error: "disconnected",
                }),
              );
            }
          }
        }
        for (const [id, pending] of pendingCompactResponses) {
          if (pending.targetName === msg.name) {
            const p = cleanupPendingCompact(id);
            if (p) {
              p.resolve(
                textResult(`Terminal "${msg.name}" disconnected`, {
                  to: msg.name,
                  error: "disconnected",
                }),
              );
            }
          }
        }
        updateStatus();
        notify(`"${msg.name}" left the link`, "info");
        break;

      // ── Status update from another terminal ──
      case "status_update":
        terminalStatuses.set(msg.name, msg.status);
        if (msg.context) terminalContexts.set(msg.name, msg.context);
        else if (msg.context === null) terminalContexts.delete(msg.name);
        resetInactivityFor(msg.name);
        break;

      // ── Chat message ──
      case "chat":
        if (msg.triggerTurn) {
          inbox.push({ from: msg.from, content: msg.content });
          scheduleFlush(FLUSH_DELAY_MS);
        } else {
          pi.sendMessage(
            {
              customType: "link",
              content: msg.content,
              display: true,
              details: { from: msg.from },
            },
            { triggerTurn: false, deliverAs: "steer" },
          );
        }
        break;

      // ── Another terminal asks us to compact our context ──
      case "compact_request": {
        if (agentRunning || pendingRemotePrompt || compactRunning) {
          routeMessage({
            type: "compact_response",
            id: msg.id,
            from: terminalName,
            to: msg.from,
            ok: false,
            reason: "busy",
          });
          break;
        }
        const { id, from } = msg;
        let finished = false;
        const finish = (ok: boolean, reason?: string) => {
          if (finished) return;
          finished = true;
          compactRunning = false;
          routeMessage({
            type: "compact_response",
            id,
            from: terminalName,
            to: from,
            ok,
            reason,
          });
        };
        if (!ctx?.compact) {
          finish(false, "unsupported");
          break;
        }
        compactRunning = true;
        notify(`"${from}" requested compact`, "info");
        // compact() aborts the current turn first, so the busy guard above
        // keeps us from interrupting active work. The runtime guarantees
        // exactly one of onComplete/onError fires, so compactRunning can't
        // get stuck and the sender won't hang.
        try {
          ctx.compact({
            customInstructions: msg.instructions,
            onComplete: () => finish(true),
            onError: (e) =>
              finish(false, e instanceof Error ? e.message : String(e)),
          });
        } catch (e) {
          finish(false, e instanceof Error ? e.message : String(e));
        }
        break;
      }

      // ── Another terminal asks us to run a prompt ──
      case "prompt_request":
        if (agentRunning || pendingRemotePrompt || compactRunning) {
          routeMessage({
            type: "prompt_response",
            id: msg.id,
            from: terminalName,
            to: msg.from,
            response: "",
            error: "Terminal is busy",
          });
        } else {
          pendingRemotePrompt = { id: msg.id, from: msg.from };
          // Keepalive: periodic status push so sender knows we're alive.
          // Keepalive presumes sendUserMessage() starts a run (platform contract);
          // if it ever doesn't, the sender's 30 min hard ceiling is the backstop.
          if (keepaliveTimer) clearInterval(keepaliveTimer);
          keepaliveTimer = setInterval(
            () => pushStatus(true),
            KEEPALIVE_INTERVAL_MS,
          );
          notify(`Running remote prompt from "${msg.from}"`, "info");
          pi.sendUserMessage(
            `[Remote prompt from "${msg.from}"]\n\n${msg.prompt}`,
          );
        }
        break;

      // ── Response to a prompt we sent ──
      case "prompt_response": {
        const pending = cleanupPending(msg.id);
        if (pending) {
          if (msg.error) {
            pending.resolve(
              textResult(`Error from "${msg.from}": ${msg.error}`, {
                from: msg.from,
                error: msg.error,
              }),
            );
          } else {
            pending.resolve(textResult(msg.response, { from: msg.from }));
          }
        }
        break;
      }

      // ── Response to a compact we requested ──
      case "compact_response": {
        const pending = cleanupPendingCompact(msg.id);
        if (pending) {
          // Use the requested target, not msg.from: a hub-synthesized
          // not_found response comes from the hub, not the worker.
          const target = pending.targetName;
          if (msg.ok) {
            pending.resolve(
              textResult(`Compacted "${target}"`, { to: target }),
            );
          } else {
            const reason = msg.reason ?? "failed";
            pending.resolve(
              textResult(`Compact on "${target}" not done: ${reason}`, {
                to: target,
                error: reason,
              }),
            );
          }
        }
        break;
      }

      case "error":
        notify(`Link: ${msg.message}`, "error");
        break;
    }
  }

  // ── Hub: handle a new client WebSocket ───────────────────────────────────

  function hubHandleClient(clientWs: WebSocket) {
    let clientName = "";

    clientWs.on("message", (raw) => {
      if (!isRuntimeLive()) return;
      const msg = safeParse(raw.toString());
      if (!msg) return;

      // First message must be register
      if (msg.type === "register") {
        if (clientName) return; // already registered — ignore duplicate
        clientName = uniqueName(msg.name);
        hubClients.set(clientWs, clientName);
        if (msg.cwd) hubTerminalCwds.set(clientName, msg.cwd);
        if (msg.context) hubTerminalContexts.set(clientName, msg.context);
        const list = terminalList();
        connectedTerminals = list;
        updateStatus();

        // Confirm to the new client (include status + cwd snapshots)
        const statuses: Record<string, LinkStatus> = {};
        statuses[terminalName] = deriveStatus(); // hub's own status
        for (const [name, status] of hubTerminalStatuses) {
          if (name !== clientName) statuses[name] = status;
        }
        const cwds: Record<string, string> = {};
        if (currentCwd) cwds[terminalName] = currentCwd; // hub's own cwd
        for (const [name, cwd] of hubTerminalCwds) {
          if (name !== clientName) cwds[name] = cwd;
        }
        const contexts: Record<string, ContextSnapshot> = {};
        const hubContext = captureContext();
        if (hubContext) contexts[terminalName] = hubContext; // hub's own context
        for (const [name, c] of hubTerminalContexts) {
          if (name !== clientName) contexts[name] = c;
        }
        clientWs.send(
          JSON.stringify({
            type: "welcome",
            name: clientName,
            terminals: list,
            statuses,
            cwds,
            contexts,
          } satisfies WelcomeMsg),
        );

        // Notify everyone else (include joiner's cwd + context)
        const joined: TerminalJoinedMsg = {
          type: "terminal_joined",
          name: clientName,
          terminals: list,
          cwd: msg.cwd,
          context: msg.context,
        };
        hubBroadcast(joined, clientName);
        return;
      }

      // Ignore messages from unregistered clients
      if (!clientName) return;

      // Status update — store and fan out to other clients only (not back to hub)
      if (msg.type === "status_update") {
        hubTerminalStatuses.set(clientName, msg.status);
        if (msg.context) hubTerminalContexts.set(clientName, msg.context);
        else if (msg.context === null) hubTerminalContexts.delete(clientName);
        resetInactivityFor(clientName);
        const normalized: StatusUpdateMsg = {
          type: "status_update",
          name: clientName,
          status: msg.status,
          context: msg.context, // undefined omitted by JSON; null forwarded to clear
        };
        const json = JSON.stringify(normalized);
        for (const [otherWs, name] of hubClients) {
          if (name !== clientName) otherWs.send(json);
        }
        return;
      }

      // Route chat / prompt messages.
      // Normalize `from` to the hub's authoritative socket→name mapping,
      // mirroring the status_update path above. Don't trust the client.
      if (
        msg.type === "chat" ||
        msg.type === "prompt_request" ||
        msg.type === "prompt_response" ||
        msg.type === "compact_request" ||
        msg.type === "compact_response"
      ) {
        routeMessage({ ...msg, from: clientName });
      }
    });

    clientWs.on("close", () => {
      if (disposed) return;
      const name = hubClients.get(clientWs);
      if (!name) return; // already removed (e.g. via disconnect) — ignore stale event
      hubClients.delete(clientWs);
      hubTerminalStatuses.delete(name);
      hubTerminalContexts.delete(name);
      hubTerminalCwds.delete(name);
      const list = terminalList();
      connectedTerminals = list;
      updateStatus();
      const left: TerminalLeftMsg = {
        type: "terminal_left",
        name,
        terminals: list,
      };
      hubBroadcast(left, name);
    });

    clientWs.on("error", () => {
      clientWs.close();
    });
  }

  // ── Start as hub ─────────────────────────────────────────────────────────

  function startHub(): Promise<boolean> {
    return new Promise((resolve) => {
      const server = new WebSocketServer({
        port: DEFAULT_PORT,
        host: "127.0.0.1",
      });

      server.on("listening", () => {
        if (disposed) {
          server.close();
          resolve(false);
          return;
        }
        wss = server;
        // If a client `/link-name` was in flight when the previous hub vanished,
        // this terminal is now establishing hub identity, so honor that pending
        // request. Otherwise keep the last hub-assigned identity — don't replay
        // a stale `preferredName` that may already have been deduped.
        if (pendingClientRename && preferredName) terminalName = preferredName;
        pendingClientRename = false;
        role = "hub";
        connectedTerminals = [terminalName];
        updateStatus();
        notify(
          `Link hub started on :${DEFAULT_PORT} as "${terminalName}"`,
          "info",
        );
        resolve(true);
      });

      server.on("connection", (clientWs) => {
        if (disposed) {
          clientWs.close();
          return;
        }
        hubHandleClient(clientWs);
      });

      server.on("error", () => {
        // Port in use → someone else is the hub
        resolve(false);
      });
    });
  }

  // ── Connect as client ────────────────────────────────────────────────────

  function connectAsClient(): Promise<boolean> {
    return new Promise((resolve) => {
      const socket = new WebSocket(`ws://127.0.0.1:${DEFAULT_PORT}`);
      let resolved = false;

      socket.on("open", () => {
        if (disposed) {
          socket.close();
          if (!resolved) {
            resolved = true;
            resolve(false);
          }
          return;
        }
        ws = socket;
        role = "client";
        resolved = true;
        // Register with preferred name if available, otherwise current name
        socket.send(
          JSON.stringify({
            type: "register",
            name: preferredName ?? terminalName,
            cwd: currentCwd || undefined,
            context: captureContext(),
          } satisfies RegisterMsg),
        );
        resolve(true);
      });

      socket.on("message", (raw) => {
        if (!isRuntimeLive()) return;
        const msg = safeParse(raw.toString());
        if (msg) handleIncoming(msg);
      });

      socket.on("close", () => {
        ws = null;
        if (disposed) return;
        if (role === "client") {
          role = "disconnected";
          connectedTerminals = [];
          updateStatus();

          if (!manuallyDisconnected) {
            notify("Disconnected from link hub", "warning");
            scheduleReconnect();
          }
        }
      });

      socket.on("error", () => {
        if (!resolved) {
          resolved = true;
          resolve(false);
        }
        socket.close();
      });
    });
  }

  // ── Initialize (auto-discover) ──────────────────────────────────────────

  async function initialize() {
    if (disposed) return;

    // Try connecting to an existing hub
    if (await connectAsClient()) return;

    // No hub found — become the hub
    if (await startHub()) return;

    // Port busy but couldn't connect (rare race). Retry after delay.
    scheduleReconnect();
  }

  function scheduleReconnect() {
    if (disposed || manuallyDisconnected || reconnectTimer) return;
    const delay = RECONNECT_DELAY_MS + Math.random() * 3000;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (role === "disconnected" && !disposed && !manuallyDisconnected)
        initialize();
    }, delay);
  }

  // ── Cleanup ──────────────────────────────────────────────────────────────

  function disconnect() {
    // Clear reconnect timer first to prevent races
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }

    // Clean up target-side remote prompt state
    if (keepaliveTimer) {
      clearInterval(keepaliveTimer);
      keepaliveTimer = null;
    }
    pendingRemotePrompt = null;
    compactRunning = false;

    // Clean up pending prompts and compacts
    for (const id of [...pendingPromptResponses.keys()]) {
      const pending = cleanupPending(id);
      if (pending) {
        pending.resolve(
          textResult("Link disconnected", { error: "disconnected" }),
        );
      }
    }
    for (const id of [...pendingCompactResponses.keys()]) {
      const pending = cleanupPendingCompact(id);
      if (pending) {
        pending.resolve(
          textResult("Link disconnected", { error: "disconnected" }),
        );
      }
    }

    // Close client connection
    if (ws) {
      ws.close();
      ws = null;
    }

    // Close hub server
    if (wss) {
      for (const clientWs of hubClients.keys()) clientWs.close();
      hubClients.clear();
      wss.close();
      wss = null;
    }

    role = "disconnected";
    connectedTerminals = [];
    terminalStatuses.clear();
    hubTerminalStatuses.clear();
    terminalContexts.clear();
    hubTerminalContexts.clear();
    terminalCwds.clear();
    hubTerminalCwds.clear();
    lastPushedKind = null;
    lastPushedTool = null;
    updateStatus();

    // Inbox survives disconnect — messages are local state waiting for local delivery.
    // Ensure pending flush still fires.
    if (inbox.length > 0 && !flushTimer) {
      scheduleFlush(FLUSH_DELAY_MS);
    }
  }

  function cleanup() {
    disposed = true;
    if (startupConnectTimer) {
      clearTimeout(startupConnectTimer);
      startupConnectTimer = null;
    }
    disconnect();
    ctx = undefined;
    // Full teardown: clear inbox and flush timer
    inbox.length = 0;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
  }

  // ── Lifecycle events ─────────────────────────────────────────────────────

  pi.on("session_start", async (_event, _ctx) => {
    ctx = _ctx;
    currentCwd = _ctx.cwd;

    // Resolve terminal name. Precedence:
    //   --link-name flag  >  PI_LINK_NAME env  >  saved link-name  >  session name  >  random
    //
    // --link-name is the public CLI surface (link identity only, never touches session name).
    // PI_LINK_NAME is the internal handoff from the `pi-link` wrapper, which DOES
    // seed session name when absent (the wrapper's combined-mode contract).
    // PI_LINK_NAME is consumed once and removed from process.env so spawned children don't inherit it.
    const cliRaw = pi.getFlag("link-name");
    let cliFlagName: string | undefined;
    if (typeof cliRaw === "string") {
      cliFlagName = normalizeName(cliRaw);
      if (!cliFlagName) {
        console.error("Error: --link-name requires a non-empty value.");
        process.exit(1);
      }
    }

    const envRaw = process.env.PI_LINK_NAME;
    delete process.env.PI_LINK_NAME;
    const envFlagName = normalizeName(envRaw);

    const flagName = cliFlagName ?? envFlagName;
    const fromEnv = !cliFlagName && !!envFlagName;

    if (flagName) {
      preferredName = flagName;
      terminalName = flagName;

      // Skip append if the saved name already matches; persistence is needed
      // only for first-time set or actual change. Reduces session-file growth
      // on repeated startups (common in automation).
      const latest = latestCustomData("link-name") as
        | { name?: unknown }
        | undefined;
      const latestSaved =
        typeof latest?.name === "string" ? latest.name : undefined;
      if (normalizeName(latestSaved) !== flagName) {
        pi.appendEntry("link-name", { name: flagName });
      }

      // Critical: only the env path (wrapper combined mode) seeds session name.
      // Public --link-name is link-only.
      if (fromEnv && !pi.getSessionName()) pi.setSessionName(flagName);
    } else {
      const saved = latestCustomData("link-name") as
        | { name?: unknown }
        | undefined;
      const savedName = normalizeName(
        typeof saved?.name === "string" ? saved.name : undefined,
      );
      if (savedName) {
        preferredName = savedName;
        terminalName = preferredName;
      } else {
        const sessionName = normalizeName(pi.getSessionName());
        if (sessionName) terminalName = sessionName;
      }
    }

    if (flagName || shouldConnect()) scheduleStartupConnect();
  });

  pi.on("session_shutdown", async () => {
    cleanup();
  });

  pi.on("agent_start", async () => {
    agentRunning = true;
    activeToolName = null;
    stateSince = Date.now();
    pushStatus();
  });

  pi.on("session_compact", async () => {
    // Tokens just dropped sharply — force a push so peers see the new context.
    pushStatus(true);
  });

  pi.on("tool_execution_start", async (event) => {
    activeToolName = event.toolName;
    stateSince = Date.now();
    pushStatus();
  });

  pi.on("tool_execution_end", async () => {
    activeToolName = null;
    if (agentRunning) stateSince = Date.now();
    pushStatus();
  });

  pi.on("agent_end", async (event) => {
    agentRunning = false;
    activeToolName = null;
    stateSince = Date.now();
    pushStatus();

    // Wake up inbox flush — agent_end fires before finishRun(), so ctx.isIdle()
    // is still false here. scheduleFlush(0) defers to next macrotask when idle.
    if (inbox.length > 0) scheduleFlush(0);

    // If we were running a remote prompt, send the response back
    if (pendingRemotePrompt) {
      const { id, from } = pendingRemotePrompt;
      if (keepaliveTimer) {
        clearInterval(keepaliveTimer);
        keepaliveTimer = null;
      }
      pendingRemotePrompt = null;

      // Find the last assistant text in this run
      let responseText = "";
      for (let i = event.messages.length - 1; i >= 0; i--) {
        const msg = event.messages[i];
        if (msg.role === "assistant") {
          responseText = msg.content
            .filter((c: { type: string }) => c.type === "text")
            .map((c: { type: string; text?: string }) => c.text ?? "")
            .join("\n");
          break;
        }
      }

      routeMessage({
        type: "prompt_response",
        id,
        from: terminalName,
        to: from,
        response: responseText || "(no response)",
      });
    }
  });

  // ── Tool helpers ──────────────────────────────────────────────────────────

  function textResult(text: string, details: Record<string, unknown> = {}) {
    return { content: [{ type: "text" as const, text }], details };
  }

  function notConnectedResult() {
    return textResult("Not connected to link", { error: "not_connected" });
  }

  function truncatePreview(text: string, max = 60) {
    return text.length > max ? text.slice(0, max) + "..." : text;
  }

  // Shared "target not found" result for the send/prompt/compact tools.
  // Returns null when the target is present, so callers can `if (miss) return miss;`.
  function targetNotFound(to: string) {
    return connectedTerminals.includes(to)
      ? null
      : textResult(
          `Terminal "${to}" not found. Connected: ${connectedTerminals.join(", ")}`,
          { to, error: "not_found" },
        );
  }

  // Shared ✓/✗ result renderer for link_send and link_compact.
  function renderIconResult(
    result: { content: { type: string; text?: string }[]; details?: unknown },
    theme: { fg(role: string, text: string): string },
  ) {
    const txt = result.content[0];
    const details = result.details as Record<string, unknown> | undefined;
    const icon = details?.error
      ? theme.fg("error", "✗ ")
      : theme.fg("success", "✓ ");
    return new Text(icon + (txt?.type === "text" ? txt.text : ""), 0, 0);
  }

  // ── Tools ────────────────────────────────────────────────────────────────

  pi.registerTool({
    name: "link_send",
    label: "Link Send",
    description: [
      "Send a message to another Pi terminal on the link.",
      'Use to:"*" for broadcast. Set triggerTurn:true to make the receiving terminal\'s LLM respond.',
    ].join(" "),
    promptSnippet:
      "Send a message to another Pi terminal on the local link network",
    parameters: Type.Object({
      to: Type.String({
        description: 'Target terminal name, or "*" for broadcast',
      }),
      message: Type.String({ description: "Message content" }),
      triggerTurn: Type.Optional(
        Type.Boolean({
          description:
            "Whether to trigger an LLM turn on the receiver (default: false)",
        }),
      ),
    }),

    async execute(_toolCallId, params) {
      if (role === "disconnected") return notConnectedResult();

      // Pre-validate target exists locally (best-effort, catches typos and definitely-absent names)
      if (params.to !== "*") {
        if (params.to === terminalName) {
          return textResult("Cannot send to yourself", {
            to: params.to,
            error: "self_target",
          });
        }
        const miss = targetNotFound(params.to);
        if (miss) return miss;
      }

      const delivered = routeMessage({
        type: "chat",
        from: terminalName,
        to: params.to,
        content: params.message,
        triggerTurn: params.triggerTurn ?? false,
      });

      const target = params.to === "*" ? "all terminals" : `"${params.to}"`;
      if (!delivered) {
        return textResult(`Failed to send to ${target}`, {
          to: params.to,
          error: "not_delivered",
        });
      }
      // Hub delivery is authoritative; client delivery is optimistic (hub routes)
      const verb = role === "hub" ? "Sent to" : "Sent to hub for delivery to";
      return textResult(`${verb} ${target}`, {
        to: params.to,
        triggerTurn: params.triggerTurn ?? false,
      });
    },

    renderCall(args, theme) {
      const target = args.to === "*" ? "broadcast" : args.to;
      const preview =
        typeof args.message === "string"
          ? truncatePreview(args.message)
          : "...";
      let text = theme.fg("toolTitle", theme.bold("link_send "));
      text += theme.fg("accent", target);
      if (args.triggerTurn) text += theme.fg("warning", " (trigger)");
      text += "\n  " + theme.fg("dim", preview);
      return new Text(text, 0, 0);
    },

    renderResult: (result, _options, theme) => renderIconResult(result, theme),
  });

  pi.registerTool({
    name: "link_compact",
    label: "Link Compact",
    description: [
      "Ask another Pi terminal to compact its context window and wait until it finishes.",
      "Returns once the target has compacted, so you can immediately send it new work.",
      "Busy targets (mid-turn or already compacting) decline; retry when idle.",
    ].join(" "),
    promptSnippet: "Ask another Pi terminal to compact its context window",
    parameters: Type.Object({
      to: Type.String({ description: "Target terminal name" }),
      instructions: Type.Optional(
        Type.String({
          description: "Optional custom compaction instructions for the target",
        }),
      ),
    }),

    async execute(_toolCallId, params, signal) {
      if (signal?.aborted) {
        return textResult("Compact request aborted", {
          to: params.to,
          error: "aborted",
        });
      }

      if (role === "disconnected") return notConnectedResult();

      if (params.to === terminalName) {
        return textResult("Cannot compact yourself - use /compact.", {
          to: params.to,
          error: "self_target",
        });
      }

      const miss = targetNotFound(params.to);
      if (miss) return miss;

      const requestId = crypto.randomUUID();

      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          const pending = cleanupPendingCompact(requestId);
          if (pending) {
            pending.resolve(
              textResult(
                `Compact request to "${params.to}" timed out (${COMPACT_TIMEOUT_MS / 1000}s)`,
                { to: params.to, error: "timeout" },
              ),
            );
          }
        }, COMPACT_TIMEOUT_MS);

        pendingCompactResponses.set(requestId, {
          resolve,
          targetName: params.to,
          timeout,
        });

        signal?.addEventListener(
          "abort",
          () => {
            const pending = cleanupPendingCompact(requestId);
            if (pending) {
              pending.resolve(
                textResult("Compact request aborted", {
                  to: params.to,
                  error: "aborted",
                }),
              );
            }
          },
          { once: true },
        );

        const delivered = routeMessage({
          type: "compact_request",
          id: requestId,
          from: terminalName,
          to: params.to,
          instructions: params.instructions,
        });

        if (!delivered) {
          const pending = cleanupPendingCompact(requestId);
          if (pending) {
            pending.resolve(
              textResult(`Failed to request compact on "${params.to}"`, {
                to: params.to,
                error: "not_delivered",
              }),
            );
          }
        }
      });
    },

    renderCall(args, theme) {
      let text = theme.fg("toolTitle", theme.bold("link_compact "));
      text += theme.fg("accent", String(args.to));
      if (typeof args.instructions === "string")
        text += "\n  " + theme.fg("dim", truncatePreview(args.instructions));
      return new Text(text, 0, 0);
    },

    renderResult: (result, _options, theme) => renderIconResult(result, theme),
  });

  pi.registerTool({
    name: "link_prompt",
    label: "Link Prompt",
    description: [
      "Send a prompt to another Pi terminal and wait for its LLM to respond.",
      "The remote terminal processes the prompt as if a user typed it,",
      "then returns the assistant's response. Times out after 90s of inactivity.",
    ].join(" "),
    promptSnippet:
      "Send a prompt to another Pi terminal and receive its LLM response",
    parameters: Type.Object({
      to: Type.String({ description: "Target terminal name" }),
      prompt: Type.String({ description: "Prompt to send" }),
    }),

    async execute(_toolCallId, params, signal) {
      if (signal?.aborted) {
        return textResult("Prompt request aborted", {
          to: params.to,
          error: "aborted",
        });
      }

      if (role === "disconnected") return notConnectedResult();

      if (params.to === terminalName) {
        return textResult("Cannot prompt yourself", {
          to: params.to,
          error: "self_target",
        });
      }

      const miss = targetNotFound(params.to);
      if (miss) return miss;

      const requestId = crypto.randomUUID();

      return new Promise((resolve) => {
        const inactivityTimeout = makeInactivityTimeout(requestId, params.to);

        const ceilingTimeout = setTimeout(() => {
          const pending = cleanupPending(requestId);
          if (pending) {
            pending.resolve(
              textResult(
                `Prompt to "${params.to}" hit hard ceiling (${PROMPT_HARD_CEILING_MS / 60_000}min)`,
                { to: params.to, error: "timeout" },
              ),
            );
          }
        }, PROMPT_HARD_CEILING_MS);

        pendingPromptResponses.set(requestId, {
          resolve,
          targetName: params.to,
          inactivityTimeout,
          ceilingTimeout,
        });

        // Abort handling
        signal?.addEventListener(
          "abort",
          () => {
            const pending = cleanupPending(requestId);
            if (pending) {
              pending.resolve(
                textResult("Prompt request aborted", {
                  to: params.to,
                  error: "aborted",
                }),
              );
            }
          },
          { once: true },
        );

        const delivered = routeMessage({
          type: "prompt_request",
          id: requestId,
          from: terminalName,
          to: params.to,
          prompt: params.prompt,
        });

        if (!delivered) {
          const pending = cleanupPending(requestId);
          if (pending) {
            pending.resolve(
              textResult(`Failed to send prompt to "${params.to}"`, {
                to: params.to,
                error: "not_delivered",
              }),
            );
          }
        }
      });
    },

    renderCall(args, theme) {
      const preview =
        typeof args.prompt === "string" ? truncatePreview(args.prompt) : "...";
      let text = theme.fg("toolTitle", theme.bold("link_prompt "));
      text += theme.fg("accent", args.to ?? "...");
      text += "\n  " + theme.fg("dim", preview);
      return new Text(text, 0, 0);
    },

    renderResult(result, _options, theme) {
      const txt = result.content[0];
      const details = result.details as Record<string, unknown> | undefined;
      if (details?.error) {
        return new Text(
          theme.fg("error", "✗ ") + (txt?.type === "text" ? txt.text : ""),
          0,
          0,
        );
      }
      const from = details?.from ?? "unknown";
      const response = txt?.type === "text" ? txt.text : "";
      const preview = truncatePreview(response, 200);
      return new Text(
        theme.fg("success", "✓ ") +
          theme.fg("accent", `[${from}] `) +
          theme.fg("text", preview),
        0,
        0,
      );
    },
  });

  pi.registerTool({
    name: "link_list",
    label: "Link List",
    description: "List all Pi terminals currently connected to the link.",
    promptSnippet: "List connected Pi terminals on the link",
    parameters: Type.Object({}),

    async execute() {
      if (role === "disconnected") return notConnectedResult();

      const statuses: Record<string, string> = {};
      const cwds: Record<string, string> = {};
      const contexts: Record<string, ContextSnapshot> = {};
      const list = connectedTerminals
        .map((name) => {
          const status = getStatusFor(name);
          const statusStr = status ? formatStatus(status) : "";
          if (statusStr) statuses[name] = statusStr;
          const cwd = getCwdFor(name);
          if (cwd) cwds[name] = cwd;
          const context = getContextFor(name);
          if (context) contexts[name] = context;
          const ctxStr = formatContext(context);
          const marker = name === terminalName ? " (you)" : "";
          let line = `  \u2022 ${name}${marker}${statusStr ? "  " + statusStr : ""}`;
          if (ctxStr) line += `  \u00b7 ${ctxStr}`;
          if (cwd) line += `\n    cwd: ${cwd}`;
          return line;
        })
        .join("\n");

      return textResult(`Connected terminals:\n${list}`, {
        terminals: connectedTerminals,
        statuses,
        cwds,
        contexts,
        self: terminalName,
        role,
      });
    },

    renderResult(result, _options, theme) {
      const details = result.details as
        | {
            terminals?: string[];
            statuses?: Record<string, string>;
            cwds?: Record<string, string>;
            contexts?: Record<string, ContextSnapshot>;
            self?: string;
            role?: string;
          }
        | undefined;
      if (!details?.terminals) {
        const txt = result.content[0];
        return new Text(txt?.type === "text" ? txt.text : "", 0, 0);
      }

      let text = theme.fg("toolTitle", theme.bold("link "));
      text += theme.fg("muted", `(${details.role}) `);
      text += theme.fg("accent", `${details.terminals.length} terminal(s)`);
      for (const name of details.terminals) {
        const isSelf = name === details.self;
        const status = details.statuses?.[name] ?? "";
        const cwd = details.cwds?.[name];
        const ctxStr = formatContext(details.contexts?.[name]);
        const nameStr = isSelf ? `\u2022 ${name} (you)` : `\u2022 ${name}`;
        text +=
          "\n  " +
          (isSelf ? theme.fg("accent", nameStr) : theme.fg("text", nameStr)) +
          (status ? "  " + theme.fg("dim", status) : "") +
          (ctxStr ? theme.fg("dim", "  \u00b7 " + ctxStr) : "");
        if (cwd) text += "\n    " + theme.fg("dim", `cwd: ${shortenPath(cwd)}`);
      }
      return new Text(text, 0, 0);
    },
  });

  // ── Commands ─────────────────────────────────────────────────────────────

  pi.registerCommand("link", {
    description: "Show link status",
    handler: async (_args, _ctx) => {
      if (role === "disconnected") {
        _ctx.ui.notify("Link: not connected", "warning");
        return;
      }
      const lines = connectedTerminals.map((name) => {
        const status = getStatusFor(name);
        const statusStr = status ? formatStatus(status) : "";
        const cwd = getCwdFor(name);
        const ctxStr = formatContext(getContextFor(name));
        const marker = name === terminalName ? " (you)" : "";
        let line = `${name}${marker}${statusStr ? ": " + statusStr : ""}`;
        if (ctxStr) line += ` \u00b7 ${ctxStr}`;
        if (cwd) line += `\n  cwd: ${shortenPath(cwd)}`;
        return line;
      });
      _ctx.ui.notify(
        `Link: ${terminalName} (${role}) · ${connectedTerminals.length} online\n${lines.join("\n")}`,
        "info",
      );
    },
  });

  pi.registerCommand("link-name", {
    description: "Change link name. No arg = use session name",
    handler: async (args, _ctx) => {
      let newName = normalizeName(args) ?? "";
      if (!newName) {
        // No argument: use session name if available
        const sessionName = normalizeName(pi.getSessionName());
        if (sessionName) {
          newName = sessionName;
        } else {
          _ctx.ui.notify(
            `Current name: "${terminalName}". No session name set. Usage: /link-name <name>`,
            "info",
          );
          return;
        }
      }

      if (newName === terminalName && newName === preferredName) {
        _ctx.ui.notify(`Already using "${newName}"`, "info");
        return;
      }

      function savePreference() {
        preferredName = newName;
        pi.appendEntry("link-name", { name: preferredName });
      }

      if (newName === terminalName) {
        savePreference();
        _ctx.ui.notify(`Saved "${newName}" as preferred link name`, "info");
        return;
      }

      // If we're the hub, check uniqueness before persisting
      if (role === "hub") {
        // Check if name is taken by another terminal
        const takenByOther = Array.from(hubClients.values()).includes(newName);
        if (takenByOther) {
          _ctx.ui.notify(
            `Name "${newName}" is already taken by another terminal`,
            "warning",
          );
          return;
        }
        const old = terminalName;
        terminalName = newName;
        const list = terminalList();
        connectedTerminals = list;
        updateStatus();
        // Notify clients only — hub already updated local state
        hubBroadcast(
          { type: "terminal_left", name: old, terminals: list },
          terminalName,
        );
        hubBroadcast(
          {
            type: "terminal_joined",
            name: newName,
            terminals: list,
            cwd: currentCwd,
            context: captureContext(),
          },
          terminalName,
        );
        pushStatus(true);
        savePreference();
        _ctx.ui.notify(`Renamed to "${newName}"`, "info");
      } else if (role === "client") {
        // Don't update terminalName here — welcome will assign authoritatively
        // after reconnect. Hub may dedupe newName to newName-2 if taken.
        savePreference();
        pendingClientRename = true;
        ws?.close();
        _ctx.ui.notify(
          `Reconnecting, requesting "${newName}" (hub may assign a different name if taken)...`,
          "info",
        );
      } else {
        savePreference();
        terminalName = newName;
        _ctx.ui.notify(`Name set to "${newName}" (not connected)`, "info");
      }
    },
  });

  pi.registerCommand("link-broadcast", {
    description: "Broadcast a message to all connected terminals",
    handler: async (args, _ctx) => {
      const message = args.trim();
      if (!message) {
        _ctx.ui.notify("Usage: /link-broadcast <message>", "warning");
        return;
      }
      if (role === "disconnected") {
        _ctx.ui.notify("Not connected to link", "warning");
        return;
      }
      routeMessage({
        type: "chat",
        from: terminalName,
        to: "*",
        content: message,
        triggerTurn: false,
      });
      _ctx.ui.notify("Broadcast sent", "info");
    },
  });

  pi.registerCommand("link-disconnect", {
    description: "Disconnect from the link",
    handler: async (_args, _ctx) => {
      pi.appendEntry("link-active", { active: false });
      manuallyDisconnected = true;
      if (role === "disconnected") {
        if (reconnectTimer) {
          clearTimeout(reconnectTimer);
          reconnectTimer = null;
        }
        _ctx.ui.notify("Link disconnected", "info");
        return;
      }
      disconnect();
      _ctx.ui.notify("Disconnected from link", "info");
    },
  });

  pi.registerCommand("link-connect", {
    description: "Connect to the link",
    handler: async (_args, _ctx) => {
      if (role !== "disconnected") {
        _ctx.ui.notify(
          `Already connected as "${terminalName}" (${role})`,
          "info",
        );
        return;
      }
      pi.appendEntry("link-active", { active: true });
      manuallyDisconnected = false;
      await initialize();
    },
  });

  // ── Message renderer ─────────────────────────────────────────────────────

  pi.registerMessageRenderer("link", (message, _options, theme) => {
    const from =
      (message.details as Record<string, unknown> | undefined)?.from ?? "link";
    const text =
      theme.fg("accent", `⚡ [${from}] `) +
      theme.fg("text", String(message.content));
    return new Text(text, 0, 0);
  });
}
