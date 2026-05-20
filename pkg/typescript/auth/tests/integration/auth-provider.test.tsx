// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { act, render, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  AuthProvider,
  clearClient,
  storageKeyFor,
  useAuth,
  ws_transport,
  type WalletConnector,
} from "@reifydb/auth";

import { WS_URL, wait_for_database } from "./setup";
import { make_test_wallet } from "./test-wallet";

interface ProbeRef {
  status: string;
  clientReady: boolean;
  error: string | null;
  wallet_address: string | null;
  token: string | null;
  identity: string | null;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
}

function Probe({ outRef }: { outRef: { current: ProbeRef | null } }) {
  const a = useAuth();
  outRef.current = {
    status: a.status,
    clientReady: a.clientReady,
    error: a.error,
    wallet_address: a.session?.wallet_address ?? null,
    token: a.session?.token ?? null,
    identity: a.session?.identity ?? null,
    signIn: a.signIn,
    signOut: a.signOut,
  };
  return null;
}

function provider(
  wallet: WalletConnector,
  namespace: string,
  ref: { current: ProbeRef | null },
) {
  return (
    <AuthProvider
      url={WS_URL}
      transport={ws_transport}
      storageNamespace={namespace}
      method="solana"
      domain="test"
      statement="Sign in to ReifyDB"
      wallet={wallet}
      sessionTtlSeconds={3600}
    >
      <Probe outRef={ref} />
    </AuthProvider>
  );
}

beforeAll(async () => {
  await wait_for_database();
}, 30000);

beforeEach(() => {
  localStorage.clear();
  clearClient();
});

afterEach(() => {
  clearClient();
});

let test_counter = 0;
function unique_namespace(): string {
  test_counter += 1;
  return `test.int.${test_counter}.${Date.now()}`;
}

describe("AuthProvider — end-to-end against testcontainer", () => {
  it("signIn drives disconnected -> authenticated with a real server-issued token", async () => {
    const { wallet, publicKeyB58 } = make_test_wallet();
    const ns = unique_namespace();
    const ref: { current: ProbeRef | null } = { current: null };
    render(provider(wallet, ns, ref));

    expect(ref.current?.status).toBe("disconnected");

    await act(async () => {
      await ref.current!.signIn();
    });
    await waitFor(
      () => {
        expect(ref.current?.status).toBe("authenticated");
        expect(ref.current?.clientReady).toBe(true);
      },
      { timeout: 10000 },
    );

    expect(ref.current?.wallet_address).toBe(publicKeyB58);
    expect(ref.current?.token?.length).toBeGreaterThan(0);
    expect(ref.current?.identity?.length).toBeGreaterThan(0);
    expect(ref.current?.error).toBeNull();
  });

  it("persists the session to localStorage on successful signIn", async () => {
    const { wallet, publicKeyB58 } = make_test_wallet();
    const ns = unique_namespace();
    const ref: { current: ProbeRef | null } = { current: null };
    render(provider(wallet, ns, ref));

    await act(async () => {
      await ref.current!.signIn();
    });
    await waitFor(
      () => expect(ref.current?.status).toBe("authenticated"),
      { timeout: 10000 },
    );

    const raw = localStorage.getItem(storageKeyFor(ns));
    expect(raw).not.toBeNull();
    const stored = JSON.parse(raw!);
    expect(stored.wallet_address).toBe(publicKeyB58);
    expect(stored.token).toBe(ref.current?.token);
    expect(stored.identity).toBe(ref.current?.identity);
    expect(stored.expires_at).toBeGreaterThan(Math.floor(Date.now() / 1000));
  });

  it("signOut tears down session, storage, and cached client", async () => {
    const { wallet } = make_test_wallet();
    const ns = unique_namespace();
    const ref: { current: ProbeRef | null } = { current: null };
    render(provider(wallet, ns, ref));

    await act(async () => {
      await ref.current!.signIn();
    });
    await waitFor(
      () => expect(ref.current?.status).toBe("authenticated"),
      { timeout: 10000 },
    );
    expect(localStorage.getItem(storageKeyFor(ns))).not.toBeNull();

    await act(async () => {
      await ref.current!.signOut();
    });

    expect(ref.current?.status).toBe("disconnected");
    expect(ref.current?.clientReady).toBe(false);
    expect(ref.current?.token).toBeNull();
    expect(localStorage.getItem(storageKeyFor(ns))).toBeNull();
  });

  it("tears down when a different wallet is connected after authentication", async () => {
    const wallet_a = make_test_wallet();
    const wallet_b = make_test_wallet();
    const ns = unique_namespace();
    const ref: { current: ProbeRef | null } = { current: null };
    const { rerender } = render(provider(wallet_a.wallet, ns, ref));

    await act(async () => {
      await ref.current!.signIn();
    });
    await waitFor(
      () => expect(ref.current?.status).toBe("authenticated"),
      { timeout: 10000 },
    );

    // Swap to a different wallet (different publicKey) by rerendering. The
    // wallet-match gate in AuthProvider must tear the session down because
    // the stored wallet_address no longer matches the live publicKey.
    rerender(provider(wallet_b.wallet, ns, ref));

    await waitFor(
      () => expect(ref.current?.status).toBe("disconnected"),
      { timeout: 5000 },
    );
    expect(localStorage.getItem(storageKeyFor(ns))).toBeNull();
  });
});
