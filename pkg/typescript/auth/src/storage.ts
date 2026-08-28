// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { AuthSession } from "./types";

const SUFFIX = ".auth";

function keyFor(namespace: string): string {
  return `${namespace}${SUFFIX}`;
}

// A per-tab id so two tabs never share one localStorage slot and stomp each
// other's session. Backed by sessionStorage: stable across reloads within a
// tab, unique per tab, gone when the tab closes. Falls back to an ephemeral
// in-memory id when sessionStorage or crypto.randomUUID is unavailable
// (degraded: the session will not survive a reload).
const TAB_ID_KEY = "reifydb.auth.tab";

function newTabId(): string {
  try {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
  } catch {
  }
  return `t-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function resolveTabId(): string {
  if (typeof window === "undefined") return "ssr";
  try {
    const existing = sessionStorage.getItem(TAB_ID_KEY);
    if (existing) return existing;
    const id = newTabId();
    sessionStorage.setItem(TAB_ID_KEY, id);
    return id;
  } catch {
    // sessionStorage unavailable (private mode, disabled): ephemeral id.
    return newTabId();
  }
}

const TAB_ID = resolveTabId();

// Scopes a caller's namespace to this browser tab. AuthProvider feeds the
// result into every storage operation so each tab gets its own slot and the
// cross-tab `storage` listener filters other tabs out by key.
export function tabScopedNamespace(namespace: string): string {
  return `${namespace}.${TAB_ID}`;
}

function isAuthSession(v: unknown): v is AuthSession {
  if (v === null || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  if (
    o.method !== undefined &&
    o.method !== "wallet" &&
    o.method !== "password" &&
    o.method !== "token"
  ) {
    return false;
  }
  if (o.identifier !== undefined && typeof o.identifier !== "string") {
    return false;
  }
  return (
    typeof o.token === "string" && o.token.length > 0 &&
    typeof o.identity === "string" && o.identity.length > 0 &&
    typeof o.walletAddress === "string" && o.walletAddress.length > 0 &&
    typeof o.expiresAt === "number" && Number.isFinite(o.expiresAt) && o.expiresAt > 0
  );
}

function safeRemove(namespace: string): void {
  try {
    localStorage.removeItem(keyFor(namespace));
  } catch {
    // localStorage may be unavailable; ignore
  }
}

export function readStoredSession(namespace: string): AuthSession | null {
  if (typeof window === "undefined") return null;
  let raw: string | null;
  try {
    raw = localStorage.getItem(keyFor(namespace));
  } catch {
    return null;
  }
  if (raw == null) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    safeRemove(namespace);
    return null;
  }
  if (!isAuthSession(parsed)) {
    safeRemove(namespace);
    return null;
  }
  if (parsed.expiresAt <= Date.now() / 1000) {
    safeRemove(namespace);
    return null;
  }
  return parsed;
}

export function writeStoredSession(namespace: string, session: AuthSession): void {
  if (typeof window === "undefined") return;
  if (!isAuthSession(session)) {
    throw new Error("@reifydb/auth: refusing to persist malformed session");
  }
  try {
    localStorage.setItem(keyFor(namespace), JSON.stringify(session));
  } catch {
    // localStorage may be unavailable (private mode, full quota); ignore.
  }
}

export function clearStoredSession(namespace: string): void {
  if (typeof window === "undefined") return;
  safeRemove(namespace);
}

export function storageKeyFor(namespace: string): string {
  return keyFor(namespace);
}

// Closed tabs leave their per-tab slot in localStorage forever - nothing ever
// reads that namespace again. Sweep once on mount: drop slots under
// `baseNamespace` that belong to other tabs and have already expired. Live
// tabs' slots have a future `expires_at` and are never touched.
export function sweepExpiredSessions(baseNamespace: string): void {
  if (typeof window === "undefined") return;
  try {
    const now = Date.now() / 1000;
    const ownKey = keyFor(tabScopedNamespace(baseNamespace));
    const prefix = `${baseNamespace}.`;
    const dead: string[] = [];
    for (let i = 0; i < localStorage.length; i += 1) {
      const k = localStorage.key(i);
      if (k == null || k === ownKey) continue;
      if (!k.startsWith(prefix) || !k.endsWith(SUFFIX)) continue;
      const raw = localStorage.getItem(k);
      if (raw == null) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        continue;
      }
      if (isAuthSession(parsed) && parsed.expiresAt <= now) {
        dead.push(k);
      }
    }
    for (const k of dead) localStorage.removeItem(k);
  } catch {
    // localStorage unavailable; nothing to sweep.
  }
}
