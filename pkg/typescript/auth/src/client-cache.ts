// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { AuthCapableClient } from "./types";
import type { AuthTransport } from "./transport";

// Module-level dedupe: in React StrictMode (dev) the AuthProvider effect runs
// twice with the same inputs, which previously opened two sockets. We cache the
// in-flight promise so both runs share one connection.

let currentKey: string | null = null;
let cachedClient: AuthCapableClient | null = null;
let currentTransport: AuthTransport | null = null;
let pendingPromise: Promise<AuthCapableClient> | null = null;

function keyOf(kind: string, url: string, token: string): string {
  return `${kind}|${url}|${token}`;
}

export async function ensureClient<T extends AuthCapableClient>(
  transport: AuthTransport<T>,
  url: string,
  token: string,
): Promise<T> {
  const key = keyOf(transport.kind, url, token);

  if (currentKey === key) {
    if (cachedClient) return cachedClient as T;
    if (pendingPromise) return pendingPromise as Promise<T>;
  }

  // New (kind, url, token) — release any prior client, start fresh.
  if (cachedClient && currentTransport) {
    try {
      (currentTransport as AuthTransport<AuthCapableClient>).release(cachedClient);
    } catch {
      // release must be idempotent; ignore
    }
    cachedClient = null;
  }
  currentKey = key;
  currentTransport = transport as AuthTransport;

  const p = transport.connect(url, token).then((client) => {
    // Another ensure/clear ran while we were connecting — drop this one.
    if (currentKey !== key) {
      try {
        transport.release(client);
      } catch {
        // ignore
      }
      throw new Error("@reifydb/auth: client connect superseded");
    }
    cachedClient = client;
    pendingPromise = null;
    return client;
  });
  pendingPromise = p as Promise<AuthCapableClient>;
  return p;
}

export function clearClient(): void {
  if (cachedClient && currentTransport) {
    try {
      (currentTransport as AuthTransport<AuthCapableClient>).release(cachedClient);
    } catch {
    }
  }
  currentKey = null;
  cachedClient = null;
  currentTransport = null;
  pendingPromise = null;
}

export function currentClient<T extends AuthCapableClient = AuthCapableClient>(): T {
  if (!cachedClient) {
    throw new Error("@reifydb/auth: no authenticated client; call ensureClient first");
  }
  return cachedClient as T;
}
