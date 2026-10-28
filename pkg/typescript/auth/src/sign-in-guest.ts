// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type {
  AuthCapableClient,
  AuthSession,
  GuestAuthCapableClient,
} from "./types";
import type { AuthTransport } from "./transport";

export interface GuestSignInArgs<
  TClient extends AuthCapableClient = AuthCapableClient,
> {
  url: string;
  transport: AuthTransport<TClient>;
  sessionTtlSeconds: number;
}

function supportsLogin(
  client: AuthCapableClient,
): client is GuestAuthCapableClient {
  return typeof (client as GuestAuthCapableClient).login === "function";
}

export async function performGuestSignIn<TClient extends AuthCapableClient>(
  args: GuestSignInArgs<TClient>,
): Promise<AuthSession> {
  const { url, transport, sessionTtlSeconds } = args;

  if (!Number.isFinite(sessionTtlSeconds) || sessionTtlSeconds <= 0) {
    throw new Error("@reifydb/auth: sessionTtlSeconds must be a positive number");
  }

  const client = await transport.connect(url);
  try {
    if (!supportsLogin(client)) {
      throw new Error("@reifydb/auth: transport client does not support login");
    }
    const auth = await client.login("guest", {});
    return {
      token: auth.token,
      identity: auth.identity,
      walletAddress: auth.identity,
      method: "guest",
      expiresAt: Math.floor(Date.now() / 1000) + sessionTtlSeconds,
    };
  } finally {
    transport.release(client);
  }
}
