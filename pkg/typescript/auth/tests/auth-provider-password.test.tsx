// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../src/auth-provider";
import { clearClient } from "../src/client-cache";
import {
  storageKeyFor,
  tabScopedNamespace,
  writeStoredSession,
} from "../src/storage";
import { useAuth } from "../src/use-auth";
import type {
  AuthSession,
  CredentialAuthCapableClient,
  PasswordCredentials,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

const NS = "test.provider.password";
const SCOPED_NS = tabScopedNamespace(NS);
const URL = "http://test";
const EMAIL = "user@example.com";

interface ProbeRef {
  status: string;
  clientReady: boolean;
  error: string | null;
  identifier: string | null;
  signIn: (credentials?: PasswordCredentials) => Promise<void>;
  signOut: () => Promise<void>;
}

function Probe({ outRef }: { outRef: { current: ProbeRef | null } }) {
  const a = useAuth();
  outRef.current = {
    status: a.status,
    clientReady: a.clientReady,
    error: a.error,
    identifier: a.session?.identifier ?? null,
    signIn: a.signIn,
    signOut: a.signOut,
  };
  return null;
}

function fake_client(): CredentialAuthCapableClient & {
  login_with_password: ReturnType<typeof vi.fn>;
  logout: ReturnType<typeof vi.fn>;
} {
  return {
    login_challenge: vi.fn(),
    login_with_password: vi
      .fn<CredentialAuthCapableClient["login_with_password"]>()
      .mockResolvedValue({ token: "tok", identity: "id" }),
    logout: vi.fn().mockResolvedValue(undefined),
  };
}

function fake_transport(
  signin_client: CredentialAuthCapableClient,
  authed_client: CredentialAuthCapableClient,
): AuthTransport {
  return {
    kind: "http",
    connect: vi.fn((_url: string, token?: string) =>
      Promise.resolve(token == null ? signin_client : authed_client),
    ),
    release: vi.fn(),
  };
}

// No wallet prop at all: this is the credential-only provider shape the
// uptime webapp uses.
function mount(
  transport: AuthTransport,
  ref: { current: ProbeRef | null },
  sessionScope?: "tab" | "browser",
) {
  return render(
    <AuthProvider
      url={URL}
      transport={transport}
      storageNamespace={NS}
      sessionTtlSeconds={3600}
      sessionScope={sessionScope}
    >
      <Probe outRef={ref} />
    </AuthProvider>,
  );
}

function password_session(over: Partial<AuthSession> = {}): AuthSession {
  return {
    token: "tok",
    identity: "id",
    wallet_address: EMAIL,
    identifier: EMAIL,
    method: "password",
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

describe("AuthProvider (password flow)", () => {
  it("signIn(credentials) reaches authenticated without any wallet", async () => {
    const signin_client = fake_client();
    const authed_client = fake_client();
    const transport = fake_transport(signin_client, authed_client);
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    expect(ref.current?.status).toBe("disconnected");
    await act(async () => {
      await ref.current?.signIn({ identifier: EMAIL, password: "hunter2" });
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
    });
    expect(ref.current?.identifier).toBe(EMAIL);
    expect(signin_client.login_with_password).toHaveBeenCalledWith(
      EMAIL,
      "hunter2",
    );
    expect(transport.connect).toHaveBeenCalledWith(URL, "tok");
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).not.toBeNull();
  });

  it("restores a stored password session on mount without a wallet", async () => {
    writeStoredSession(SCOPED_NS, password_session());
    const transport = fake_transport(fake_client(), fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
      expect(ref.current?.clientReady).toBe(true);
    });
    expect(transport.connect).toHaveBeenCalledWith(URL, "tok");
  });

  it("tears down a stored wallet session when no wallet prop is given", async () => {
    // A wallet-bound session (no method field = legacy wallet session) cannot
    // be proven without a wallet connector, so a credential-only provider must
    // refuse to resurrect it instead of silently authenticating.
    writeStoredSession(
      SCOPED_NS,
      password_session({ method: undefined, identifier: undefined, wallet_address: "WalletA" }),
    );
    const transport = fake_transport(fake_client(), fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
    expect(transport.connect).not.toHaveBeenCalled();
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("signIn surfaces wrong-credential errors and persists nothing", async () => {
    const signin_client = fake_client();
    signin_client.login_with_password.mockRejectedValueOnce(
      new Error("invalid credentials"),
    );
    const transport = fake_transport(signin_client, fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await act(async () => {
      await ref.current?.signIn({ identifier: EMAIL, password: "wrong" });
    });
    expect(ref.current?.status).toBe("error");
    expect(ref.current?.error).toBe("invalid credentials");
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("signIn() without credentials and without a wallet errors instead of throwing", async () => {
    const transport = fake_transport(fake_client(), fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref);

    await act(async () => {
      await ref.current?.signIn();
    });
    expect(ref.current?.status).toBe("error");
    expect(ref.current?.error).toMatch(/not connected/i);
  });

  it("sessionScope browser persists under the bare namespace shared by all tabs", async () => {
    const transport = fake_transport(fake_client(), fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref, "browser");

    await act(async () => {
      await ref.current?.signIn({ identifier: EMAIL, password: "hunter2" });
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    // The whole point of browser scope: the shared `${NS}.auth` slot is used,
    // not a per-tab slot, so a new tab starts logged in.
    expect(localStorage.getItem(`${NS}.auth`)).not.toBeNull();
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });

  it("cross-tab sign-out tears down a browser-scoped session", async () => {
    writeStoredSession(NS, password_session());
    const transport = fake_transport(fake_client(), fake_client());
    const ref: { current: ProbeRef | null } = { current: null };
    mount(transport, ref, "browser");

    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    act(() => {
      window.dispatchEvent(
        new StorageEvent("storage", {
          key: storageKeyFor(NS),
          newValue: null,
          oldValue: "{...}",
        }),
      );
    });
    await waitFor(() => {
      expect(ref.current?.status).toBe("disconnected");
    });
  });

  it("signOut revokes server-side, clears storage, and disconnects", async () => {
    const authed_client = fake_client();
    const transport = fake_transport(fake_client(), authed_client);
    const ref: { current: ProbeRef | null } = { current: null };
    writeStoredSession(SCOPED_NS, password_session());
    mount(transport, ref);

    await waitFor(() => {
      expect(ref.current?.status).toBe("authenticated");
    });
    await act(async () => {
      await ref.current?.signOut();
    });
    expect(authed_client.logout).toHaveBeenCalled();
    expect(ref.current?.status).toBe("disconnected");
    expect(localStorage.getItem(storageKeyFor(SCOPED_NS))).toBeNull();
  });
});
