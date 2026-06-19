// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { AuthCapableClient } from "./types";
import type { AuthTransport } from "./transport";

// Module-level dedupe: in React StrictMode (dev) the AuthProvider effect runs
// twice with the same inputs, which previously opened two sockets. We cache the
// in-flight promise so both runs share one connection.

let current_key: string | null = null;
let current_client: AuthCapableClient | null = null;
let current_transport: AuthTransport | null = null;
let pending_promise: Promise<AuthCapableClient> | null = null;

function key_of(kind: string, url: string, token: string): string {
  return `${kind}|${url}|${token}`;
}

export async function ensureClient<T extends AuthCapableClient>(
  transport: AuthTransport<T>,
  url: string,
  token: string,
): Promise<T> {
  const key = key_of(transport.kind, url, token);

  if (current_key === key) {
    if (current_client) return current_client as T;
    if (pending_promise) return pending_promise as Promise<T>;
  }

  // New (kind, url, token) — release any prior client, start fresh.
  if (current_client && current_transport) {
    try {
      (current_transport as AuthTransport<AuthCapableClient>).release(current_client);
    } catch {
      // release must be idempotent; ignore
    }
    current_client = null;
  }
  current_key = key;
  current_transport = transport as AuthTransport;

  const p = transport.connect(url, token).then((client) => {
    // Another ensure/clear ran while we were connecting — drop this one.
    if (current_key !== key) {
      try {
        transport.release(client);
      } catch {
        // ignore
      }
      throw new Error("@reifydb/auth: client connect superseded");
    }
    current_client = client;
    pending_promise = null;
    return client;
  });
  pending_promise = p as Promise<AuthCapableClient>;
  return p;
}

export function clearClient(): void {
  if (current_client && current_transport) {
    try {
      (current_transport as AuthTransport<AuthCapableClient>).release(current_client);
    } catch {
      // ignore
    }
  }
  current_key = null;
  current_client = null;
  current_transport = null;
  pending_promise = null;
}

export function currentClient<T extends AuthCapableClient = AuthCapableClient>(): T {
  if (!current_client) {
    throw new Error("@reifydb/auth: no authenticated client; call ensureClient first");
  }
  return current_client as T;
}
