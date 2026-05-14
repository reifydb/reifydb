// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { AuthSession } from "./types";

const SUFFIX = ".auth";

function key_for(namespace: string): string {
  return `${namespace}${SUFFIX}`;
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
