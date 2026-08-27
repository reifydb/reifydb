// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, waitFor } from "@testing-library/react";
import React, { type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../src/auth-provider";
import { clearClient } from "../src/client-cache";
import { storageKeyFor, tabScopedNamespace, writeStoredSession } from "../src/storage";
import { useAuth } from "../src/use-auth";
import type {
  AuthCapableClient,
  AuthSession,
  WalletConnector,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

const NS = "test.provider";
// AuthProvider scopes its storage per tab; tests that poke localStorage
// directly must use the same tab-scoped namespace the provider derives.
const SCOPED_NS = tabScopedNamespace(NS);
const URL = "ws://test";
const WALLET_A = "WalletA0000000000000000000000000000000000000";
const WALLET_B = "WalletB0000000000000000000000000000000000000";

interface ProbeRef {
  status: string;
  clientReady: boolean;
  error: string | null;
  walletAddress: string | null;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
}

function Probe({ outRef }: { outRef: { current: ProbeRef | null } }) {
  const a = useAuth();
  outRef.current = {
    status: a.status,
    clientReady: a.clientReady,
    error: a.error,
    walletAddress: a.session?.walletAddress ?? null,
    signIn: a.signIn,
    signOut: a.signOut,
  };
  return null;
}

function fakeClient(): AuthCapableClient & {
  loginChallenge: ReturnType<typeof vi.fn>;
  logout: ReturnType<typeof vi.fn>;
} {
  return {
    loginChallenge: vi.fn(),
    logout: vi.fn().mockResolvedValue(undefined),
  };
}

function fakeTransport(
  signinClient: AuthCapableClient,
  authedClient: AuthCapableClient,
): AuthTransport {
  return {
    kind: "ws",
    connect: vi.fn((_url: string, token?: string) =>
      Promise.resolve(token == null ? signinClient : authedClient),
    ),
    release: vi.fn(),
  };
}

function fakeWallet(over: Partial<WalletConnector> = {}): WalletConnector {
  return {
    connected: false,
    connecting: false,
    publicKey: null,
    hasSelectedWallet: false,
    async signMessage(_msg: Uint8Array) {
      return new Uint8Array([1, 2, 3]);
    },
    encodeSignature(bytes: Uint8Array) {
      return `sig:${bytes.length}`;
    },
    ...over,
  };
}

function mount(
  wallet: WalletConnector,
  transport: AuthTransport,
  ref: { current: ProbeRef | null },
  children?: ReactNode,
) {
  return render(
    <AuthProvider
      url={URL}
      transport={transport}
      storageNamespace={NS}
      method="solana"
      domain="d"
      statement="s"
      wallet={wallet}
      sessionTtlSeconds={3600}
    >
      <Probe outRef={ref} />
      {children}
    </AuthProvider>,
  );
}

beforeEach(() => {
  localStorage.clear();
  clearClient();
});

afterEach(() => {
  clearClient();
});

function futureSession(over: Partial<AuthSession> = {}): AuthSession {
  return {
    token: "tok",
    identity: "id",
    walletAddress: WALLET_A,
    expiresAt: Math.floor(Date.now() / 1000) + 3600,
    ...over,
  };
}

describe("AuthProvider", () => {
  it("mounts disconnected when no session is stored", () => {
    const ref: { current: ProbeRef | null } = { current: null };
    mount(fakeWallet(), fakeTransport(fakeClient(), fakeClient()), ref);
    expect(ref.current?.status).toBe("disconnected");
    expect(ref.current?.clientReady).toBe(false);
  });

  it("transitions verifying -> authenticated when stored wallet matches", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    const transport = fakeTransport(fakeClient(), fakeClient());
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      transport,
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
    });
    expect(transport.connect).toHaveBeenCalledWith(URL, "tok");
  });

  it("tears down to disconnected when stored wallet mismatches connected wallet", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    const transport = fakeTransport(fakeClient(), fakeClient());
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_B, hasSelectedWallet: true }),
      transport,
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
    expect(ref.current?.clientReady).toBe(false);
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("stays in verifying while wallet is autoConnecting", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    const transport = fakeTransport(fakeClient(), fakeClient());
    mount(
      fakeWallet({ connected: false, connecting: true, hasSelectedWallet: true }),
      transport,
      ref,
    );
    expect(ref.current?.status).toBe("verifying");
    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("tears down when stored session present but no wallet is selected", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    mount(fakeWallet({ connected: false, hasSelectedWallet: false }), fakeTransport(fakeClient(), fakeClient()), ref);
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("tears down on cross-tab storage clear", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      fakeTransport(fakeClient(), fakeClient()),
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(SCOPED_NS),
          newValue: null,
          oldValue: "{...}",
        }),
      );
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
  });

  it("keeps the session on a cross-tab token rotation for the same principal", async () => {
    // Regression: a second tab signing in with the same wallet mints a fresh
    // token and rewrites our storage slot. That is not an intrusion, so we must
    // stay authenticated rather than tear every other tab down.
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      fakeTransport(fakeClient(), fakeClient()),
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(SCOPED_NS),
          newValue: JSON.stringify(
            futureSession({ walletAddress: WALLET_A, identity: "id", token: "tok-rotated" }),
          ),
        }),
      );
    });
    expect(ref.current?.status).toBe("authenticated");
    expect(ref.current?.clientReady).toBe(true);
  });

  it("tears down on a cross-tab takeover by a different wallet", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A }));
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      fakeTransport(fakeClient(), fakeClient()),
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(SCOPED_NS),
          newValue: JSON.stringify(
            futureSession({ walletAddress: WALLET_B, identity: "id", token: "tok-b" }),
          ),
        }),
      );
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
  });

  it("tears down on a cross-tab takeover by a different identity", async () => {
    writeStoredSession(SCOPED_NS, futureSession({ walletAddress: WALLET_A, identity: "id" }));
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      fakeTransport(fakeClient(), fakeClient()),
      ref,
    );
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(SCOPED_NS),
          newValue: JSON.stringify(
            futureSession({ walletAddress: WALLET_A, identity: "id-other", token: "tok-2" }),
          ),
        }),
      );
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
  });

  it("ignores a cross-tab write while we hold no session and leaves storage intact", async () => {
    // Regression: a tab that is not signed in must not clear the slot another
    // tab just wrote - doing so bounces that tab straight back out.
    const ref: { current: ProbeRef | null } = { current: null };
    mount(fakeWallet(), fakeTransport(fakeClient(), fakeClient()), ref);
    expect(ref.current?.status).toBe("disconnected");

    const written = JSON.stringify(futureSession({ walletAddress: WALLET_A }));
    localStorage.setItem(storageKeyFor(SCOPED_NS), written);
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(SCOPED_NS),
          newValue: written,
        }),
      );
    });
    expect(ref.current?.status).toBe("disconnected");
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBe(written);
  });

  it("signIn happy path: disconnected -> verifying -> authenticated", async () => {
    const signinClient = fakeClient();
    signinClient.loginChallenge
      .mockResolvedValueOnce({
        kind: "challenge",
        challengeId: "c1",
        message: "msg",
        nonce: "n",
      })
      .mockResolvedValueOnce({
        kind: "authenticated",
        token: "tok",
        identity: "id",
      });
    const authedClient = fakeClient();
    const transport = fakeTransport(signinClient, authedClient);

    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({
        connected: true,
        publicKey: WALLET_A,
        hasSelectedWallet: true,
      }),
      transport,
      ref,
    );

    expect(ref.current?.status).toBe("disconnected");
    await act(async () => {
      await ref.current?.signIn();
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
      expect(ref.current?.walletAddress).toBe(WALLET_A);
    });
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).not.toBeNull();
  });

  it("signIn rejects when wallet is not connected", async () => {
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: false }),
      fakeTransport(fakeClient(), fakeClient()),
      ref,
    );
    await act(async () => {
      await ref.current?.signIn();
    });
    expect(ref.current?.status).toBe("error");
    expect(ref.current?.error).toMatch(/not connected/i);
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("signIn surfaces transport errors and does not persist a session", async () => {
    const signinClient = fakeClient();
    signinClient.loginChallenge.mockRejectedValueOnce(new Error("network down"));
    const transport = fakeTransport(signinClient, fakeClient());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({
        connected: true,
        publicKey: WALLET_A,
        hasSelectedWallet: true,
      }),
      transport,
      ref,
    );
    await act(async () => {
      await ref.current?.signIn();
    });
    expect(ref.current?.status).toBe("error");
    expect(ref.current?.error).toBe("network down");
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("persists the session under a tab-scoped key, not the bare namespace", async () => {
    // The fix for the multi-tab logout bug: each tab writes its own slot
    // `${NS}.${tabId}.auth`, never the shared `${NS}.auth`, so concurrent
    // sign-ins in different tabs cannot stomp each other.
    const signinClient = fakeClient();
    signinClient.loginChallenge
      .mockResolvedValueOnce({
        kind: "challenge",
        challengeId: "c1",
        message: "msg",
        nonce: "n",
      })
      .mockResolvedValueOnce({
        kind: "authenticated",
        token: "tok",
        identity: "id",
      });
    const ref: { current: ProbeRef | null } = { current: null };
    mount(
      fakeWallet({ connected: true, publicKey: WALLET_A, hasSelectedWallet: true }),
      fakeTransport(signinClient, fakeClient()),
      ref,
    );
    await act(async () => {
      await ref.current?.signIn();
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    expect(localStorage.getItem(`${NS}.auth`)).toBeNull();
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).not.toBeNull();
    expect(storageKeyFor(SCOPED_NS)).toMatch(new RegExp(`^${NS}\\..+\\.auth$`));
  });
});
