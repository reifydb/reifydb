// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { useContext } from "react";

import { AuthContext, type AuthContextValue } from "./auth-provider";
import { currentClient } from "./client-cache";
import type { AuthCapableClient } from "./types";

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (ctx == null) {
    throw new Error("@reifydb/auth: useAuth must be used inside <AuthProvider>");
  }
  return ctx;
}

export function useAuthClient<
  TClient extends AuthCapableClient = AuthCapableClient,
>(): TClient {
  const { clientReady } = useAuth();
  if (!clientReady) {
    throw new Error("@reifydb/auth: useAuthClient called before clientReady");
  }
  return currentClient<TClient>();
}
