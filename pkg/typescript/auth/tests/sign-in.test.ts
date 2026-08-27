// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it, vi } from "vitest";

import { performSignIn } from "../src/sign-in";
import type {
  AuthCapableClient,
  LoginChallengeResult,
  WalletConnector,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

function makeStubClient(
  challenge: LoginChallengeResult,
  authed: LoginChallengeResult,
): AuthCapableClient & { loginChallenge: ReturnType<typeof vi.fn> } {
  const fn = vi
    .fn<AuthCapableClient["loginChallenge"]>()
    .mockResolvedValueOnce(challenge)
    .mockResolvedValueOnce(authed);
  return {
    loginChallenge: fn,
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

function makeWallet(
  publicKey: string | null,
  encodeSignature = (bytes: Uint8Array): string => `enc:${bytes.length}`,
): Pick<WalletConnector, "publicKey" | "signMessage" | "encodeSignature"> {
  return {
    publicKey,
    signMessage: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
    encodeSignature,
  };
}

describe.each([
  ["ws", "ws" as const],
  ["http", "http" as const],
])("performSignIn (%s)", (_label, kind) => {
  it("walks challenge -> sign -> submit and returns a session", async () => {
    const client = makeStubClient(
      { kind: "challenge", challengeId: "c1", message: "hello", nonce: "n1" },
      { kind: "authenticated", token: "tok", identity: "id" },
    );
    const transport = makeTransport(client, kind);
    const wallet = makeWallet("WaLLeT");

    const session = await performSignIn({
      url: "ws://test",
      transport,
      method: "solana",
      wallet,
      domain: "example.com",
      statement: "Sign in",
      sessionTtlSeconds: 60,
    });

    expect(session.token).toBe("tok");
    expect(session.identity).toBe("id");
    expect(session.walletAddress).toBe("WaLLeT");
    expect(session.expiresAt).toBeGreaterThan(Math.floor(Date.now() / 1000));

    // Two loginChallenge calls in order: first the request, then the response.
    expect(client.loginChallenge).toHaveBeenCalledTimes(2);
    const [firstCall, secondCall] = client.loginChallenge.mock.calls;
    expect(firstCall[0]).toBe("solana");
    expect(firstCall[1]).toMatchObject({
      identifier: "WaLLeT",
      public_key: "WaLLeT",
      domain: "example.com",
      statement: "Sign in",
    });
    expect(secondCall[1]).toMatchObject({
      challenge_id: "c1",
      signature: "enc:3",
      signed_message: "hello",
    });

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("releases the transient client even when the second call fails", async () => {
    const fn = vi
      .fn<AuthCapableClient["loginChallenge"]>()
      .mockResolvedValueOnce({
        kind: "challenge",
        challengeId: "c1",
        message: "hello",
        nonce: "n1",
      })
      .mockRejectedValueOnce(new Error("boom"));
    const client: AuthCapableClient = {
      loginChallenge: fn,
      logout: vi.fn().mockResolvedValue(undefined),
    };
    const transport = makeTransport(client, kind);
    const wallet = makeWallet("W");

    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet,
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/boom/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects when first response is not a challenge", async () => {
    const client = makeStubClient(
      { kind: "authenticated", token: "t", identity: "i" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = makeTransport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: makeWallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/expected challenge/);
  });

  it("rejects when second response is not authenticated", async () => {
    const client = makeStubClient(
      { kind: "challenge", challengeId: "c", message: "m", nonce: "n" },
      { kind: "challenge", challengeId: "c2", message: "m2", nonce: "n2" },
    );
    const transport = makeTransport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: makeWallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/expected authenticated/);
  });

  it("rejects when publicKey is null", async () => {
    const client = makeStubClient(
      { kind: "challenge", challengeId: "c", message: "m", nonce: "n" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = makeTransport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: makeWallet(null),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/publicKey is required/);
  });

  it("rejects when sessionTtlSeconds is not positive", async () => {
    const client = makeStubClient(
      { kind: "challenge", challengeId: "c", message: "m", nonce: "n" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = makeTransport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: makeWallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 0,
      }),
    ).rejects.toThrow(/sessionTtlSeconds/);
  });
});
