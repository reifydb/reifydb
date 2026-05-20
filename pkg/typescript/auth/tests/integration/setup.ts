// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { Client } from "@reifydb/client";

export const WS_URL = process.env.REIFYDB_WS_URL || "ws://127.0.0.1:18090";
export const HTTP_URL = process.env.REIFYDB_HTTP_URL || "http://127.0.0.1:18091";

export async function wait_for_database(
  max_retries = 30,
  delay = 1000,
): Promise<void> {
  for (let i = 0; i < max_retries; i++) {
    let client = null;
    try {
      client = await Client.connect_ws(WS_URL, { timeout_ms: 5000 });
      return;
    } catch (error) {
      if (i === max_retries - 1) {
        throw new Error(`${WS_URL} not ready after ${max_retries} attempts`);
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
