// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { render, waitFor } from "@testing-library/react";
import React, { useEffect, useState } from "react";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { WalletProvider, useWallet } from "@solana/wallet-adapter-react";
import { Shape } from "@reifydb/core";
import {
  performSignIn,
  ws_transport,
  type AuthSession,
} from "@reifydb/auth";
import { useSolanaWalletConnector } from "@reifydb/auth-solana";

import { WS_URL, wait_for_database } from "./setup";
import { MockSolanaWalletAdapter } from "./mock-adapter";

interface DriverResult {
  session?: AuthSession;
  error?: string;
}

function SignInDriver({
  onResult,
}: {
  onResult: (r: DriverResult) => void;
}) {
  const { connected, publicKey } = useWallet();
  const connector = useSolanaWalletConnector();
  const [started, setStarted] = useState(false);

  useEffect(() => {
    if (!connected || publicKey == null || started) return;
    setStarted(true);
    performSignIn({
      url: WS_URL,
      transport: ws_transport,
      method: "solana",
      wallet: connector,
      domain: "test",
      statement: "Sign in to ReifyDB",
      sessionTtlSeconds: 3600,
    })
      .then((session) => onResult({ session }))
      .catch((err) =>
        onResult({ error: err instanceof Error ? err.message : String(err) }),
      );
  }, [connected, publicKey, connector, started, onResult]);

  return null;
}

beforeAll(async () => {
  await wait_for_database();
}, 30000);

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  localStorage.clear();
});

describe("useSolanaWalletConnector — end-to-end with mock wallet adapter", () => {
  it("signs in against the testcontainer via the React hook + adapter", async () => {
    // Prime wallet-adapter-react's autoConnect: it reads the selected wallet
    // name from localStorage on mount. The same key is what the auth-solana
    // connector's hasSelectedWallet check reads.
    localStorage.setItem("walletName", JSON.stringify("Mock"));

    const adapter = new MockSolanaWalletAdapter();
    const expected_pubkey = adapter.keypair.publicKey.toBase58();
    let result: DriverResult | null = null;

    render(
      <WalletProvider wallets={[adapter]} autoConnect>
        <SignInDriver
          onResult={(r) => {
            result = r;
          }}
        />
      </WalletProvider>,
    );

    await waitFor(
      () => {
        expect(result).not.toBeNull();
      },
      { timeout: 15000 },
    );

    expect(result?.error).toBeUndefined();
    expect(result?.session).toBeDefined();
    expect(result?.session?.wallet_address).toBe(expected_pubkey);
    expect(result?.session?.token.length).toBeGreaterThan(0);
    expect(result?.session?.identity.length).toBeGreaterThan(0);
    expect(result?.session?.expires_at).toBeGreaterThan(
      Math.floor(Date.now() / 1000),
    );

    // Prove the issued token is actually accepted by the server for an
    // authenticated query, not just shape-matched at handshake time.
    const authed = await ws_transport.connect(WS_URL, result!.session!.token);
    try {
      const frames = await authed.query(
        "MAP {v: 42}",
        {},
        [Shape.object({ v: Shape.number() })],
      );
      expect(frames[0][0].v).toBe(42);
    } finally {
      ws_transport.release(authed);
    }
  });
});
