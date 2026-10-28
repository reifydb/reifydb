// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it, vi } from "vitest";

import { performGuestSignIn } from "../src/sign-in-guest";
import type { AuthCapableClient, GuestAuthCapableClient } from "../src/types";
import type { AuthTransport } from "../src/transport";

function makeGuestClient(): GuestAuthCapableClient & {
  login: ReturnType<typeof vi.fn>;
} {
  return {
    loginChallenge: vi.fn(),
    login: vi
      .fn<GuestAuthCapableClient["login"]>()
      .mockResolvedValue({ token: "guest-tok", identity: "guest-id" }),
    logout: vi.fn().mockResolvedValue(undefined),
  };
}

function makeTransport(
  client: AuthCapableClient,
  kind: "ws" | "http",
): AuthTransport & { release: ReturnType<typeof vi.fn> } {
  return {
    kind,
    connect: vi.fn().mockResolvedValue(client),
    release: vi.fn(),
  };
}

describe.each([
  ["ws", "ws" as const],
  ["http", "http" as const],
])("performGuestSignIn (%s)", (_label, kind) => {
  it("logs in with the guest method and returns a guest session", async () => {
    // The engine mints the guest only for method "guest"; storage needs a non-empty walletAddress, so it carries the identity.
    const client = makeGuestClient();
    const transport = makeTransport(client, kind);

    const session = await performGuestSignIn({ url: "u", transport, sessionTtlSeconds: 60 });

    expect(client.login).toHaveBeenCalledExactlyOnceWith("guest", {});
    expect(session.token).toBe("guest-tok");
    expect(session.identity).toBe("guest-id");
    expect(session.walletAddress).toBe("guest-id");
    expect(session.method).toBe("guest");
    expect(session.expiresAt).toBeGreaterThan(Math.floor(Date.now() / 1000));
    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("connects without a caller token", async () => {
    // A guest is never minted on behalf of another session; a stale token here could block the login.
    const client = makeGuestClient();
    const transport = makeTransport(client, kind);

    await performGuestSignIn({ url: "u", transport, sessionTtlSeconds: 60 });

    expect(transport.connect).toHaveBeenCalledExactlyOnceWith("u");
  });

  it("rejects clients without login and still releases", async () => {
    // A challenge-only client must fail loudly instead of silently degrading.
    const client: AuthCapableClient = {
      loginChallenge: vi.fn(),
      logout: vi.fn().mockResolvedValue(undefined),
    };
    const transport = makeTransport(client, kind);

    await expect(
      performGuestSignIn({ url: "u", transport, sessionTtlSeconds: 60 }),
    ).rejects.toThrow(/does not support login/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("propagates login failures and still releases the client", async () => {
    // A method-not-enabled server must surface as an error, never as a session.
    const client = makeGuestClient();
    client.login.mockRejectedValueOnce(new Error("method not enabled"));
    const transport = makeTransport(client, kind);

    await expect(
      performGuestSignIn({ url: "u", transport, sessionTtlSeconds: 60 }),
    ).rejects.toThrow(/method not enabled/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects a non-positive sessionTtlSeconds before connecting", async () => {
    const client = makeGuestClient();
    const transport = makeTransport(client, kind);

    await expect(
      performGuestSignIn({ url: "u", transport, sessionTtlSeconds: 0 }),
    ).rejects.toThrow(/sessionTtlSeconds/);

    expect(transport.connect).not.toHaveBeenCalled();
  });
});
