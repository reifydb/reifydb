// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { clearStoredSession, readStoredSession, writeStoredSession } from '@reifydb/auth'
import { ReifyError } from '@reifydb/client'

export const SESSION_NAMESPACE = 'place'
export const SESSION_TTL_SECONDS = 30 * 86400

export interface GuestLogin {
  login(method: string, credentials: Record<string, string>): Promise<{ token: string; identity: string }>
}

export interface GuestSession {
  ensure(): Promise<void>
  signUp(identifier: string, password: string): Promise<void>
}

export function guestSession(client: GuestLogin, signedIn: boolean, now: () => number = Date.now): GuestSession {
  let signingIn: Promise<void> | null = signedIn ? Promise.resolve() : null
  return {
    ensure() {
      signingIn ??= client.login('guest', {}).then(
        ({ token, identity }) => {
          writeStoredSession(SESSION_NAMESPACE, {
            token,
            identity,
            walletAddress: identity,
            method: 'guest',
            expiresAt: Math.floor(now() / 1000) + SESSION_TTL_SECONDS,
          })
        },
        (error: unknown) => {
          signingIn = null
          throw error
        },
      )
      return signingIn
    },
    async signUp(identifier, password) {
      await signingIn?.catch(() => undefined)
      const { token, identity } = await client.login('password', { identifier, password })
      writeStoredSession(SESSION_NAMESPACE, {
        token,
        identity,
        walletAddress: identity,
        method: 'password',
        identifier,
        expiresAt: Math.floor(now() / 1000) + SESSION_TTL_SECONDS,
      })
      signingIn = Promise.resolve()
    },
  }
}

export function signedInName(): string | null {
  const stored = readStoredSession(SESSION_NAMESPACE)
  return stored?.method === 'password' ? (stored.identifier ?? null) : null
}

export async function openConnection<C>(connect: (token: string | undefined) => Promise<C>): Promise<{ client: C; signedIn: boolean }> {
  const stored = readStoredSession(SESSION_NAMESPACE)
  if (stored) {
    try {
      return { client: await connect(stored.token), signedIn: true }
    } catch (error) {
      if (!(error instanceof ReifyError && error.code === 'AUTH_FAILED')) throw error
      clearStoredSession(SESSION_NAMESPACE)
    }
  }
  return { client: await connect(undefined), signedIn: false }
}
