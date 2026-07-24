// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../src/auth-provider";
import { clearClient } from "../src/client-cache";
import {
  readStoredSession,
  storageKeyFor,
  tabScopedNamespace,
  writeStoredSession,
} from "../src/storage";
import { useAuth } from "../src/use-auth";
import type { AuthCapableClient, AuthSession } from "../src/types";
import type { AuthTransport } from "../src/transport";

const NS = "test.provider.token";
const SCOPED_NS = tabScopedNamespace(NS);
const URL = "http://test";
const IDENTITY = "0199aa00-0000-7000-8000-000000000001";

interface ProbeRef {
  status: string;
  clientReady: boolean;
  error: string | null;
  identity: string | null;
  adoptSession: (session: AuthSession) => void;
  signOut: () => Promise<void>;
}

function Probe({ outRef }: { outRef: { current: ProbeRef | null } }) {
  const a = useAuth();
  outRef.current = {
    status: a.status,
    clientReady: a.clientReady,
    error: a.error,
    identity: a.session?.identity ?? null,
    adoptSession: a.adoptSession,
    signOut: a.signOut,
  };
  return null;
}

function fake_client(): AuthCapableClient {
  return {
    login_challenge: vi.fn(),
    logout: vi.fn().mockResolvedValue(undefined),
  };
}

function fake_transport(client: AuthCapableClient): AuthTransport {
  return {
    kind: "http",
    connect: vi.fn(() => Promise.resolve(client)),
    release: vi.fn(),
  };
}

function mount(transport: AuthTransport, ref: { current: ProbeRef | null }) {
  return render(
    <AuthProvider
      url={URL}
      transport={transport}
      storageNamespace={NS}
      sessionTtlSeconds={3600}
      sessionScope="browser"
    >
      <Probe outRef={ref} />
    </AuthProvider>,
  );
}

// The shape a server-minted guest session arrives in: a bare token bound to an
// identity, with no credential and no wallet behind it.
function token_session(over: Partial<AuthSession> = {}): AuthSession {
  return {
    token: "guest-token",
    identity: IDENTITY,
    wallet_address: IDENTITY,
    method: "token",
    expires_at: Math.floor(Date.now() / 1000) + 3600,
    ...over,
  };
}

beforeEach(() => {
  localStorage.clear();
  clearClient();
});

afterEach(() => {
  clearClient();
});

describe("AuthProvider (adopted token session)", () => {
  it("adoptSession reaches authenticated and connects with the token", async () => {
    const client = fake_client();
    const transport = fake_transport(client);
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    expect(ref.current?.status).toBe("disconnected");
    act(() => {
      ref.current?.adoptSession(token_session());
    });

    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
    });
    expect(ref.current?.identity).toBe(IDENTITY);
    expect(transport.connect).toHaveBeenCalledWith(URL, "guest-token");
  });

  it("persists the adopted session so a reload keeps the same identity", async () => {
    // A guest only exists as long as this token survives: losing it on reload
    // would strand everything the guest created behind an unreachable identity.
    const transport = fake_transport(fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    act(() => {
      ref.current?.adoptSession(token_session());
    });
    await waitFor(() => expect(ref.current?.status).toBe("authenticated"));

    const stored = readStoredSession(NS);
    expect(stored?.token).toBe("guest-token");
    expect(stored?.identity).toBe(IDENTITY);
    expect(stored?.method).toBe("token");
  });

  it("restores a stored token session on mount without any wallet", async () => {
    writeStoredSession(NS, token_session());
    const transport = fake_transport(fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
    });
    expect(transport.connect).toHaveBeenCalledWith(URL, "guest-token");
  });

  it("drops an expired token session instead of restoring it", async () => {
    writeStoredSession(
      NS,
      token_session({ expires_at: Math.floor(Date.now() / 1000) - 1 }),
    );
    const transport = fake_transport(fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await waitFor(() => expect(ref.current?.status).toBe("disconnected"));
    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("tears down the adopted session on signOut", async () => {
    const transport = fake_transport(fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    act(() => {
      ref.current?.adoptSession(token_session());
    });
    await waitFor(() => expect(ref.current?.status).toBe("authenticated"));

    await act(async () => {
      await ref.current?.signOut();
    });
    expect(ref.current?.status).toBe("disconnected");
    expect(localStorage.getItem(storageKeyFor(NS))).toBeNull();
  });

  it("refuses to persist a session with an unknown method", async () => {
    // writeStoredSession is the last gate before localStorage; a malformed
    // method would come back out of storage and be trusted on the next mount.
    expect(() =>
      writeStoredSession(NS, {
        ...token_session(),
        method: "smoke-signal",
      } as unknown as AuthSession),
    ).toThrow();
  });
});
