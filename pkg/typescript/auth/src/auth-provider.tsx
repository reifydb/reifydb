// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import React, {
  createContext,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
} from "react";

import { clearClient, currentClient, ensureClient } from "./client-cache";
import { performSignIn } from "./sign-in";
import { performPasswordSignIn } from "./sign-in-password";
import {
  clearStoredSession,
  readStoredSession,
  storageKeyFor,
  sweepExpiredSessions,
  tabScopedNamespace,
  writeStoredSession,
} from "./storage";
import type {
  AuthCapableClient,
  AuthSession,
  AuthState,
  PasswordCredentials,
  WalletConnector,
} from "./types";
import type { AuthTransport } from "./transport";

const DEFAULT_TTL_SECONDS = 86400;

export interface AuthContextValue extends AuthState {
  signIn(credentials?: PasswordCredentials): Promise<void>;
  signOut(): Promise<void>;
}

export const AuthContext = createContext<AuthContextValue | null>(null);

export interface AuthProviderProps<
  TClient extends AuthCapableClient = AuthCapableClient,
> {
  url: string;
  transport: AuthTransport<TClient>;
  storageNamespace: string;
  method?: string;
  domain?: string;
  statement?: string;
  sessionTtlSeconds?: number;
  wallet?: WalletConnector;
  sessionScope?: "tab" | "browser";
  children: ReactNode;
}

interface InternalState {
  status: AuthState["status"];
  session: AuthSession | null;
  clientReady: boolean;
  error: string | null;
}

export function AuthProvider<TClient extends AuthCapableClient>(
  props: AuthProviderProps<TClient>,
): ReactElement {
  const {
    url,
    transport,
    storageNamespace,
    method,
    domain,
    statement,
    sessionTtlSeconds = DEFAULT_TTL_SECONDS,
    wallet,
    sessionScope = "tab",
    children,
  } = props;

  // Per-tab storage slot: each tab gets its own localStorage key so concurrent
  // sign-ins in different tabs cannot stomp each other's session. With
  // sessionScope "browser" all tabs deliberately share one slot instead.
  const effective_namespace =
    sessionScope === "browser"
      ? storageNamespace
      : tabScopedNamespace(storageNamespace);

  const [state, setState] = useState<InternalState>(() => {
    const stored = readStoredSession(effective_namespace);
    return stored
      ? { status: "verifying", session: stored, clientReady: false, error: null }
      : { status: "disconnected", session: null, clientReady: false, error: null };
  });

  // Pull live wallet fields into stable primitives for effect deps.
  const wallet_connected = wallet?.connected ?? false;
  const wallet_connecting = wallet?.connecting ?? false;
  const wallet_public_key = wallet?.publicKey ?? null;
  const wallet_has_selected = wallet?.hasSelectedWallet ?? false;

  // Refs so callbacks can read latest values without re-binding.
  const session_ref = useRef(state.session);
  session_ref.current = state.session;
  const wallet_ref = useRef(wallet);
  wallet_ref.current = wallet;

  const tear_down = useCallback(
    (next_status: InternalState["status"], next_error: string | null) => {
      clearStoredSession(effective_namespace);
      clearClient();
      setState({
        status: next_status,
        session: null,
        clientReady: false,
        error: next_error,
      });
    },
    [effective_namespace],
  );

  // Session gate. Password sessions connect directly; wallet sessions keep the
  // security invariant: the authenticated client is only ever constructed when
  // the connected wallet matches the persisted session.
  useEffect(() => {
    const session = state.session;
    if (session == null) return;

    const establish = () => {
      let cancelled = false;
      ensureClient(transport, url, session.token)
        .then(() => {
          if (cancelled) return;
          setState((prev) =>
            prev.session === session
              ? { ...prev, status: "authenticated", clientReady: true, error: null }
              : prev,
          );
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          const message = err instanceof Error ? err.message : "Failed to connect client";
          tear_down("error", message);
        });
      return () => {
        cancelled = true;
      };
    };

    if (session.method === "password") {
      return establish();
    }

    if (wallet_connected && wallet_public_key != null) {
      if (wallet_public_key !== session.wallet_address) {
        tear_down("disconnected", null);
        return;
      }
      return establish();
    }

    if (!wallet_connected && (wallet_connecting || wallet_has_selected)) {
      // Wallet adapter is autoConnecting; stay in verifying. Do NOT construct
      // the client yet - we must not authenticate before the match is confirmed.
      setState((prev) =>
        prev.session === session
          ? { ...prev, status: "verifying", clientReady: false }
          : prev,
      );
      return;
    }

    // Wallet is definitively absent (no selected wallet, not connecting). User
    // explicitly disconnected, so the session can no longer be proven; tear down.
    tear_down("disconnected", null);
  }, [
    state.session,
    wallet_connected,
    wallet_connecting,
    wallet_public_key,
    wallet_has_selected,
    transport,
    url,
    tear_down,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const key = storageKeyFor(effective_namespace);
    const on_storage = (e: StorageEvent) => {
      if (e.key !== key) return;
      const current = session_ref.current;

      // Another tab signed out / cleared the session.
      if (e.newValue == null) {
        if (current != null) tear_down("disconnected", null);
        return;
      }

      let parsed: Partial<AuthSession>;
      try {
        parsed = JSON.parse(e.newValue) as Partial<AuthSession>;
      } catch {
        // Corrupt entry written by another tab. Only react if we actually hold
        // a session; otherwise there is nothing to defend.
        if (current != null) tear_down("disconnected", null);
        return;
      }

      // We hold no session of our own: nothing to defend. Clearing storage here
      // would clobber the session the other tab just wrote (and bounce it back
      // out through this same handler). Let our own sign-in / autoConnect flow
      // converge independently.
      if (current == null) return;

      // A genuinely different principal took over our storage slot.
      if (
        parsed.wallet_address !== current.wallet_address ||
        parsed.identity !== current.identity
      ) {
        tear_down("disconnected", null);
      }

      // Same principal, different token: a concurrent sign-in in another tab,
      // not an intrusion. Keep our own still-valid client.
    };
    window.addEventListener("storage", on_storage);
    return () => {
      window.removeEventListener("storage", on_storage);
    };
  }, [effective_namespace, tear_down]);

  // Housekeeping: drop expired per-tab slots left behind by closed tabs.
  useEffect(() => {
    sweepExpiredSessions(storageNamespace);
  }, [storageNamespace]);

  const signIn = useCallback(async (credentials?: PasswordCredentials) => {
    if (credentials != null) {
      setState((prev) => ({ ...prev, status: "signing", error: null }));
      try {
        const session = await performPasswordSignIn({
          url,
          transport,
          identifier: credentials.identifier,
          password: credentials.password,
          sessionTtlSeconds,
        });
        writeStoredSession(effective_namespace, session);
        setState({
          status: "verifying",
          session,
          clientReady: false,
          error: null,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Sign in failed";
        setState({
          status: "error",
          session: null,
          clientReady: false,
          error: message,
        });
      }
      return;
    }

    const w = wallet_ref.current;
    if (w == null || !w.connected || w.publicKey == null) {
      setState((prev) => ({ ...prev, status: "error", error: "Wallet not connected" }));
      return;
    }
    if (method == null || domain == null || statement == null) {
      setState((prev) => ({
        ...prev,
        status: "error",
        error: "Wallet sign in requires method, domain, and statement",
      }));
      return;
    }
    setState((prev) => ({ ...prev, status: "signing", error: null }));
    try {
      const session = await performSignIn({
        url,
        transport,
        method,
        wallet: w,
        domain,
        statement,
        sessionTtlSeconds,
      });
      // Sanity: signed-in wallet must match the live publicKey. If the user
      // swapped wallets while the signature was in flight, refuse the session.
      if (session.wallet_address !== wallet_ref.current?.publicKey) {
        throw new Error("Wallet changed during sign in");
      }
      writeStoredSession(effective_namespace, session);
      setState({
        status: "verifying",
        session,
        clientReady: false,
        error: null,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Sign in failed";
      setState({
        status: "error",
        session: null,
        clientReady: false,
        error: message,
      });
    }
  }, [
    url,
    transport,
    method,
    domain,
    statement,
    sessionTtlSeconds,
    effective_namespace,
  ]);

  const signOut = useCallback(async () => {
    // Best-effort server-side logout via the currently-cached client.
    try {
      await currentClient().logout();
    } catch {
      // no current client, or server-side logout failed; tear down regardless
    }
    tear_down("disconnected", null);
  }, [tear_down]);

  const value = useMemo<AuthContextValue>(
    () => ({
      status: state.status,
      session: state.session,
      clientReady: state.clientReady,
      error: state.error,
      signIn,
      signOut,
    }),
    [state, signIn, signOut],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
