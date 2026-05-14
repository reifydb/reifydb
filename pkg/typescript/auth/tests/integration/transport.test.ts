// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { beforeAll, describe, expect, it } from "vitest";
import {
  http_transport,
  json_http_transport,
  json_ws_transport,
  performSignIn,
  ws_transport,
  type AuthTransport,
} from "@reifydb/auth";

import { HTTP_URL, WS_URL, wait_for_database } from "./setup";
import { make_test_wallet } from "./test-wallet";

interface TransportCase {
  name: string;
  transport: AuthTransport;
  url: string;
}

const cases: TransportCase[] = [
  { name: "ws_transport", transport: ws_transport, url: WS_URL },
  { name: "http_transport", transport: http_transport, url: HTTP_URL },
  { name: "json_ws_transport", transport: json_ws_transport, url: WS_URL },
  { name: "json_http_transport", transport: json_http_transport, url: HTTP_URL },
];

describe("performSignIn — solana auto-provision via every transport", () => {
  beforeAll(async () => {
    await wait_for_database();
  }, 30000);

  for (const { name, transport, url } of cases) {
    describe(name, () => {
      it("signs in with a fresh keypair and receives a session token", async () => {
        const { wallet, publicKeyB58 } = make_test_wallet();
        const before = Math.floor(Date.now() / 1000);

        const session = await performSignIn({
          url,
          transport,
          method: "solana",
          wallet,
          domain: "test",
          statement: "Sign in to ReifyDB",
          sessionTtlSeconds: 3600,
        });

        expect(typeof session.token).toBe("string");
        expect(session.token.length).toBeGreaterThan(0);
        expect(typeof session.identity).toBe("string");
        expect(session.identity.length).toBeGreaterThan(0);
        expect(session.wallet_address).toBe(publicKeyB58);
        // expires_at is set by performSignIn as floor(now_secs) + ttl; allow 1s
        // slack for the second boundary crossing.
        expect(session.expires_at).toBeGreaterThanOrEqual(before + 3600);
        expect(session.expires_at).toBeLessThanOrEqual(before + 3600 + 2);
      });

      it("reconnects with the returned token via connect(url, token)", async () => {
        const { wallet } = make_test_wallet();

        const session = await performSignIn({
          url,
          transport,
          method: "solana",
          wallet,
          domain: "test",
          statement: "Sign in to ReifyDB",
          sessionTtlSeconds: 3600,
        });

        const client = await transport.connect(url, session.token);
        try {
          expect(client).toBeDefined();
        } finally {
          transport.release(client);
        }
      });

      it("each sign in produces a distinct identity per keypair", async () => {
        const a = make_test_wallet();
        const b = make_test_wallet();

        const sessionA = await performSignIn({
          url,
          transport,
          method: "solana",
          wallet: a.wallet,
          domain: "test",
          statement: "Sign in to ReifyDB",
          sessionTtlSeconds: 3600,
        });
        const sessionB = await performSignIn({
          url,
          transport,
          method: "solana",
          wallet: b.wallet,
          domain: "test",
          statement: "Sign in to ReifyDB",
          sessionTtlSeconds: 3600,
        });

        // Distinct wallets -> distinct server-side identities and tokens.
        expect(sessionA.wallet_address).not.toBe(sessionB.wallet_address);
        expect(sessionA.identity).not.toBe(sessionB.identity);
        expect(sessionA.token).not.toBe(sessionB.token);
      });
    });
  }
});
