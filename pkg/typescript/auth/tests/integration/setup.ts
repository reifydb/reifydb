// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Client } from "@reifydb/client";

export const WS_URL = process.env.REIFYDB_WS_URL || "ws://127.0.0.1:18090";
export const HTTP_URL = process.env.REIFYDB_HTTP_URL || "http://127.0.0.1:18091";

export async function waitForDatabase(
  maxRetries = 30,
  delay = 1000,
): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    let client = null;
    try {
      client = await Client.connectWs(WS_URL, { timeoutMs: 5000 });
      return;
    } catch (error) {
      if (i === maxRetries - 1) {
        throw new Error(`${WS_URL} not ready after ${maxRetries} attempts`);
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    } finally {
      if (client) {
        try {
          client.disconnect();
        } catch (e) {
          // ignore
        }
      }
    }
  }
}
