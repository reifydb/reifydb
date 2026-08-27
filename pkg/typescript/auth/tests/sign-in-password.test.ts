// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it, vi } from "vitest";

import { performPasswordSignIn } from "../src/sign-in-password";
import type {
  AuthCapableClient,
  CredentialAuthCapableClient,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

function makePasswordClient(): CredentialAuthCapableClient & {
  loginWithPassword: ReturnType<typeof vi.fn>;
} {
  return {
    loginChallenge: vi.fn(),
    loginWithPassword: vi
      .fn<CredentialAuthCapableClient["loginWithPassword"]>()
      .mockResolvedValue({ token: "tok", identity: "id" }),
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
])("performPasswordSignIn (%s)", (_label, kind) => {
  it("logs in with identifier + password and returns a password session", async () => {
    const client = makePasswordClient();
    const transport = makeTransport(client, kind);

    const session = await performPasswordSignIn({
      url: "u",
      transport,
      identifier: "user@example.com",
      password: "hunter2",
      sessionTtlSeconds: 60,
    });

    // identifier must survive as the principal binding for cross-tab comparison, and method must mark password sessions.
    expect(session.token).toBe("tok");
    expect(session.identity).toBe("id");
    expect(session.walletAddress).toBe("user@example.com");
    expect(session.identifier).toBe("user@example.com");
    expect(session.method).toBe("password");
    expect(session.expiresAt).toBeGreaterThan(Math.floor(Date.now() / 1000));

    expect(client.loginWithPassword).toHaveBeenCalledExactlyOnceWith(
      "user@example.com",
      "hunter2",
    );
    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("propagates wrong-credential failures and still releases the client", async () => {
    const client = makePasswordClient();
    client.loginWithPassword.mockRejectedValueOnce(
      new Error("invalid credentials"),
    );
    const transport = makeTransport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "wrong",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/invalid credentials/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects clients without loginWithPassword and still releases", async () => {
    // A minimal AuthCapableClient (challenge-only) must fail loudly instead of silently degrading.
    const client: AuthCapableClient = {
      loginChallenge: vi.fn(),
      logout: vi.fn().mockResolvedValue(undefined),
    };
    const transport = makeTransport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "hunter2",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/does not support loginWithPassword/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects an empty identifier before connecting", async () => {
    const client = makePasswordClient();
    const transport = makeTransport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "",
        password: "hunter2",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/identifier is required/);

    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("rejects an empty password before connecting", async () => {
    const client = makePasswordClient();
    const transport = makeTransport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/password is required/);

    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("rejects when sessionTtlSeconds is not positive", async () => {
    const client = makePasswordClient();
    const transport = makeTransport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "hunter2",
        sessionTtlSeconds: 0,
      }),
    ).rejects.toThrow(/sessionTtlSeconds/);
  });
});
