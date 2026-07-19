// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type {
  AuthCapableClient,
  AuthSession,
  CredentialAuthCapableClient,
} from "./types";
import type { AuthTransport } from "./transport";

export interface PasswordSignInArgs<
  TClient extends AuthCapableClient = AuthCapableClient,
> {
  url: string;
  transport: AuthTransport<TClient>;
  identifier: string;
  password: string;
  sessionTtlSeconds: number;
}

function supports_password_login(
  client: AuthCapableClient,
): client is CredentialAuthCapableClient {
  return (
    typeof (client as CredentialAuthCapableClient).login_with_password ===
    "function"
  );
}

export async function performPasswordSignIn<TClient extends AuthCapableClient>(
  args: PasswordSignInArgs<TClient>,
): Promise<AuthSession> {
  const { url, transport, identifier, password, sessionTtlSeconds } = args;

  if (identifier.length === 0) {
    throw new Error("@reifydb/auth: identifier is required to sign in");
  }
  if (password.length === 0) {
    throw new Error("@reifydb/auth: password is required to sign in");
  }
  if (!Number.isFinite(sessionTtlSeconds) || sessionTtlSeconds <= 0) {
    throw new Error("@reifydb/auth: sessionTtlSeconds must be a positive number");
  }

  const client = await transport.connect(url);
  try {
    if (!supports_password_login(client)) {
      throw new Error(
        "@reifydb/auth: transport client does not support login_with_password",
      );
    }
    const auth = await client.login_with_password(identifier, password);
    return {
      token: auth.token,
      identity: auth.identity,
      wallet_address: identifier,
      identifier,
      method: "password",
      expires_at: Math.floor(Date.now() / 1000) + sessionTtlSeconds,
    };
  } finally {
    transport.release(client);
  }
}
