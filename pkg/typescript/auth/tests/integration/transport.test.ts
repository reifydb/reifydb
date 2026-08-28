// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, describe, expect, it } from "vitest";
import {
  httpTransport,
  jsonHttpTransport,
  jsonWsTransport,
  performSignIn,
  wsTransport,
  type AuthTransport,
} from "@reifydb/auth";

import { HTTP_URL, WS_URL, waitForDatabase } from "./setup";
import { makeTestWallet } from "./test-wallet";

interface TransportCase {
  name: string;
  transport: AuthTransport;
  url: string;
}

const cases: TransportCase[] = [
  { name: "wsTransport", transport: wsTransport, url: WS_URL },
  { name: "httpTransport", transport: httpTransport, url: HTTP_URL },
  { name: "jsonWsTransport", transport: jsonWsTransport, url: WS_URL },
  { name: "jsonHttpTransport", transport: jsonHttpTransport, url: HTTP_URL },
];

describe("performSignIn — solana auto-provision via every transport", () => {
  beforeAll(async () => {
    await waitForDatabase();
  }, 30000);

  for (const { name, transport, url } of cases) {
    describe(name, () => {
      it("signs in with a fresh keypair and receives a session token", async () => {
        const { wallet, publicKeyB58 } = makeTestWallet();
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
        expect(session.walletAddress).toBe(publicKeyB58);
        // expiresAt is floor(now_secs) + ttl; allow 1s slack for the second boundary crossing.
        expect(session.expiresAt).toBeGreaterThanOrEqual(before + 3600);
        expect(session.expiresAt).toBeLessThanOrEqual(before + 3600 + 2);
      });

      it("reconnects with the returned token via connect(url, token)", async () => {
        const { wallet } = makeTestWallet();

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
        const a = makeTestWallet();
        const b = makeTestWallet();

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
        expect(sessionA.walletAddress).not.toBe(sessionB.walletAddress);
        expect(sessionA.identity).not.toBe(sessionB.identity);
        expect(sessionA.token).not.toBe(sessionB.token);
      });
    });
  }
});
