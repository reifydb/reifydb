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

function releaseWithDisconnect(client: { disconnect?: () => void }): void {
  if (typeof client.disconnect === "function") {
    try {
      client.disconnect();
    } catch {
    }
  }
}

export const wsTransport: AuthTransport<WsClient> = {
  kind: "ws",
  async connect(url, token) {
    return Client.connectWs(
      url,
      token != null ? { format: "rbcf", token } : { format: "rbcf" },
    );
  },
  release(client) {
    releaseWithDisconnect(client);
  },
};

export const httpTransport: AuthTransport<HttpClient> = {
  kind: "http",
  async connect(url, token) {
    return Client.connectHttp(
      url,
      token != null ? { format: "rbcf", token } : { format: "rbcf" },
    );
  },
  release(_client) {
  },
};

export const jsonWsTransport: AuthTransport<JsonWsClient> = {
  kind: "ws",
  async connect(url, token) {
    return Client.connectJsonWs(url, token != null ? { token } : undefined);
  },
  release(client) {
    releaseWithDisconnect(client);
  },
};

export const jsonHttpTransport: AuthTransport<JsonHttpClient> = {
  kind: "http",
  async connect(url, token) {
    return Client.connectJsonHttp(url, token != null ? { token } : undefined);
  },
  release(_client) {
  },
};
