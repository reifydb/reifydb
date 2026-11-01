// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { clearStoredSession, readStoredSession, writeStoredSession } from '@reifydb/auth'
import { ReifyError } from '@reifydb/client'

export const SESSION_NAMESPACE = 'canvas'
export const SESSION_TTL_SECONDS = 30 * 86400

export interface GuestLogin {
  login(method: string, credentials: Record<string, string>): Promise<{ token: string; identity: string }>
  command(rql: string, params: Record<string, never>, shapes: []): Promise<unknown>
}

export interface GuestSession {
  signUp(identifier: string, password: string): Promise<void>
}

export function guestSession(client: GuestLogin, now: () => number = Date.now): GuestSession {
  return {
    async signUp(identifier, password) {
      const { token, identity } = await client.login('password', { identifier, password })
      await client.command('CALL canvas::register_human()', {}, [])
      writeStoredSession(SESSION_NAMESPACE, {
        token,
        identity,
        walletAddress: identity,
        method: 'password',
        identifier,
        expiresAt: Math.floor(now() / 1000) + SESSION_TTL_SECONDS,
      })
    },
  }
}

export function signedInName(): string | null {
  const stored = readStoredSession(SESSION_NAMESPACE)
  return stored?.method === 'password' ? (stored.identifier ?? null) : null
}

export async function openConnection<C>(connect: (token: string | undefined) => Promise<C>): Promise<{ client: C }> {
  const stored = readStoredSession(SESSION_NAMESPACE)
  if (stored) {
    try {
      return { client: await connect(stored.token) }
    } catch (error) {
      if (!(error instanceof ReifyError && error.code === 'AUTH_FAILED')) throw error
      clearStoredSession(SESSION_NAMESPACE)
    }
  }
  return { client: await connect(undefined) }
}
