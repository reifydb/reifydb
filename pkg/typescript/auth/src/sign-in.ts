// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { AuthCapableClient, AuthSession, WalletConnector } from "./types";
import type { AuthTransport } from "./transport";

export interface SignInArgs<TClient extends AuthCapableClient = AuthCapableClient> {
  url: string;
  transport: AuthTransport<TClient>;
  method: string;
  wallet: Pick<WalletConnector, "publicKey" | "signMessage" | "encodeSignature">;
  domain: string;
  statement: string;
  sessionTtlSeconds: number;
}

export async function performSignIn<TClient extends AuthCapableClient>(
  args: SignInArgs<TClient>,
): Promise<AuthSession> {
  const { url, transport, method, wallet, domain, statement, sessionTtlSeconds } = args;

  const wallet_address = wallet.publicKey;
  if (wallet_address == null || wallet_address.length === 0) {
    throw new Error("@reifydb/auth: wallet.publicKey is required to sign in");
  }
  if (!Number.isFinite(sessionTtlSeconds) || sessionTtlSeconds <= 0) {
    throw new Error("@reifydb/auth: sessionTtlSeconds must be a positive number");
  }

  const client = await transport.connect(url);
  try {
    const challenge = await client.login_challenge(method, {
      identifier: wallet_address,
      public_key: wallet_address,
      domain,
      statement,
    });
    if (challenge.kind !== "challenge") {
      throw new Error(
        `@reifydb/auth: expected challenge response, got ${challenge.kind}`,
      );
    }

    const messageBytes = new TextEncoder().encode(challenge.message);
    const signatureBytes = await wallet.signMessage(messageBytes);
    const signature = wallet.encodeSignature(signatureBytes);

    const auth = await client.login_challenge(method, {
      challenge_id: challenge.challenge_id,
      signature,
      signed_message: challenge.message,
    });
    if (auth.kind !== "authenticated") {
      throw new Error(
        `@reifydb/auth: expected authenticated response, got ${auth.kind}`,
      );
    }

    return {
      token: auth.token,
      identity: auth.identity,
      wallet_address,
      expires_at: Math.floor(Date.now() / 1000) + sessionTtlSeconds,
    };
  } finally {
    transport.release(client);
  }
}
