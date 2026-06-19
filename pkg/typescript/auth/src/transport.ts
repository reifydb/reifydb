// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  Client,
  type WsClient,
  type HttpClient,
  type JsonWsClient,
  type JsonHttpClient,
} from "@reifydb/client";
import type { AuthCapableClient } from "./types";

export interface AuthTransport<
  TClient extends AuthCapableClient = AuthCapableClient,
> {
  readonly kind: "ws" | "http";
  connect(url: string, token?: string): Promise<TClient>;
  release(client: TClient): void;
}

function release_with_disconnect(client: { disconnect?: () => void }): void {
  if (typeof client.disconnect === "function") {
    try {
      client.disconnect();
    } catch {
      // disconnect should be idempotent and never throw, but guard anyway
    }
  }
}

export const ws_transport: AuthTransport<WsClient> = {
  kind: "ws",
  async connect(url, token) {
    return Client.connect_ws(
      url,
      token != null ? { format: "rbcf", token } : { format: "rbcf" },
    );
  },
  release(client) {
    release_with_disconnect(client);
  },
};

export const http_transport: AuthTransport<HttpClient> = {
  kind: "http",
  async connect(url, token) {
    return Client.connect_http(
      url,
      token != null ? { format: "rbcf", token } : { format: "rbcf" },
    );
  },
  release(_client) {
    // HttpClient has no persistent socket; nothing to release.
  },
};

export const json_ws_transport: AuthTransport<JsonWsClient> = {
  kind: "ws",
  async connect(url, token) {
    return Client.connect_json_ws(url, token != null ? { token } : undefined);
  },
  release(client) {
    release_with_disconnect(client);
  },
};

export const json_http_transport: AuthTransport<JsonHttpClient> = {
  kind: "http",
  async connect(url, token) {
    return Client.connect_json_http(url, token != null ? { token } : undefined);
  },
  release(_client) {
    // JsonHttpClient has no persistent socket; nothing to release.
  },
};
