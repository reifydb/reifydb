// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { AuthSession } from "./types";

const SUFFIX = ".auth";

function key_for(namespace: string): string {
  return `${namespace}${SUFFIX}`;
}

// A per-tab id so two tabs never share one localStorage slot and stomp each
// other's session. Backed by sessionStorage: stable across reloads within a
// tab, unique per tab, gone when the tab closes. Falls back to an ephemeral
// in-memory id when sessionStorage or crypto.randomUUID is unavailable
// (degraded: the session will not survive a reload).
const TAB_ID_KEY = "reifydb.auth.tab";

function new_tab_id(): string {
  try {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
  } catch {
    // crypto unavailable; fall through to the cheap fallback
  }
  return `t-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function resolve_tab_id(): string {
  if (typeof window === "undefined") return "ssr";
  try {
    const existing = sessionStorage.getItem(TAB_ID_KEY);
    if (existing) return existing;
    const id = new_tab_id();
    sessionStorage.setItem(TAB_ID_KEY, id);
    return id;
  } catch {
    // sessionStorage unavailable (private mode, disabled): ephemeral id.
    return new_tab_id();
  }
}

const TAB_ID = resolve_tab_id();

// Scopes a caller's namespace to this browser tab. AuthProvider feeds the
// result into every storage operation so each tab gets its own slot and the
// cross-tab `storage` listener filters other tabs out by key.
export function tabScopedNamespace(namespace: string): string {
  return `${namespace}.${TAB_ID}`;
}

function is_auth_session(v: unknown): v is AuthSession {
  if (v === null || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return (
    typeof o.token === "string" && o.token.length > 0 &&
    typeof o.identity === "string" && o.identity.length > 0 &&
    typeof o.wallet_address === "string" && o.wallet_address.length > 0 &&
    typeof o.expires_at === "number" && Number.isFinite(o.expires_at) && o.expires_at > 0
  );
}

function safe_remove(namespace: string): void {
  try {
    localStorage.removeItem(key_for(namespace));
  } catch {
    // localStorage may be unavailable; ignore
  }
}

export function readStoredSession(namespace: string): AuthSession | null {
  if (typeof window === "undefined") return null;
  let raw: string | null;
  try {
    raw = localStorage.getItem(key_for(namespace));
  } catch {
    return null;
  }
  if (raw == null) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    safe_remove(namespace);
    return null;
  }
  if (!is_auth_session(parsed)) {
    safe_remove(namespace);
    return null;
  }
  if (parsed.expires_at <= Date.now() / 1000) {
    safe_remove(namespace);
    return null;
  }
  return parsed;
}

export function writeStoredSession(namespace: string, session: AuthSession): void {
  if (typeof window === "undefined") return;
  if (!is_auth_session(session)) {
    throw new Error("@reifydb/auth: refusing to persist malformed session");
  }
  try {
    localStorage.setItem(key_for(namespace), JSON.stringify(session));
  } catch {
    // localStorage may be unavailable (private mode, full quota); ignore.
  }
}

export function clearStoredSession(namespace: string): void {
  if (typeof window === "undefined") return;
  safe_remove(namespace);
}

export function storageKeyFor(namespace: string): string {
  return key_for(namespace);
}

// Closed tabs leave their per-tab slot in localStorage forever - nothing ever
// reads that namespace again. Sweep once on mount: drop slots under
// `baseNamespace` that belong to other tabs and have already expired. Live
// tabs' slots have a future `expires_at` and are never touched.
export function sweepExpiredSessions(baseNamespace: string): void {
  if (typeof window === "undefined") return;
  try {
    const now = Date.now() / 1000;
    const own_key = key_for(tabScopedNamespace(baseNamespace));
    const prefix = `${baseNamespace}.`;
    const dead: string[] = [];
    for (let i = 0; i < localStorage.length; i += 1) {
      const k = localStorage.key(i);
      if (k == null || k === own_key) continue;
      if (!k.startsWith(prefix) || !k.endsWith(SUFFIX)) continue;
      const raw = localStorage.getItem(k);
      if (raw == null) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        continue;
      }
      if (is_auth_session(parsed) && parsed.expires_at <= now) {
        dead.push(k);
      }
    }
    for (const k of dead) localStorage.removeItem(k);
  } catch {
    // localStorage unavailable; nothing to sweep.
  }
}
