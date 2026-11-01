// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { readStoredSession, writeStoredSession } from '@reifydb/auth'
import { ReifyError } from '@reifydb/client'
import { SESSION_NAMESPACE, SESSION_TTL_SECONDS, guestSession, openConnection, signedInName } from '@/session'

const NOW = 1_800_000_000_000

function refused(code: string): ReifyError {
  return new ReifyError({ id: 'auth-1', type: 'Err', payload: { diagnostic: { code, message: 'InvalidToken', notes: [] } } })
}

beforeEach(() => {
  localStorage.clear()
})

describe('open connection', () => {
  it('connects with the stored token', async () => {
    // Without the token the server treats a returning painter as anonymous, and its next commit is refused.
    writeStoredSession(SESSION_NAMESPACE, { token: 'stored', identity: 'g', walletAddress: 'g', method: 'guest', expiresAt: Date.now() / 1000 + 60 })
    const connect = vi.fn(async (token: string | undefined) => ({ token }))

    const opened = await openConnection(connect)

    expect(connect).toHaveBeenCalledTimes(1)
    expect(connect).toHaveBeenCalledWith('stored')
    expect(opened).toEqual({ client: { token: 'stored' } })
  })

  it('drops a dead stored token and connects anonymous instead', async () => {
    // After an in-memory backend restart every stored token is dead; keeping it would break even watching the canvas.
    writeStoredSession(SESSION_NAMESPACE, { token: 'dead', identity: 'g', walletAddress: 'g', method: 'guest', expiresAt: Date.now() / 1000 + 60 })
    const connect = vi.fn(async (token: string | undefined) => {
      if (token) throw refused('AUTH_FAILED')
      return { token }
    })

    const opened = await openConnection(connect)

    expect(connect.mock.calls).toEqual([['dead'], [undefined]])
    expect(opened).toEqual({ client: { token: undefined } })
    expect(readStoredSession(SESSION_NAMESPACE)).toBeNull()
  })

  it('keeps the stored session and fails loud when connect fails for another reason', async () => {
    // A network error is not a dead token; clearing the session on it would log people out on every blip.
    writeStoredSession(SESSION_NAMESPACE, { token: 'stored', identity: 'g', walletAddress: 'g', method: 'guest', expiresAt: Date.now() / 1000 + 60 })
    const connect = vi.fn(async () => {
      throw new Error('WebSocket connection failed')
    })

    await expect(openConnection(connect)).rejects.toThrow('WebSocket connection failed')
    expect(connect).toHaveBeenCalledTimes(1)
    expect(readStoredSession(SESSION_NAMESPACE)?.token).toBe('stored')
  })

  it('connects anonymous when nothing is stored', async () => {
    // Watchers must never need a login just to see the canvas.
    const connect = vi.fn(async (token: string | undefined) => ({ token }))
    expect(await openConnection(connect)).toEqual({ client: { token: undefined } })
  })
})

describe('sign up', () => {
  function client(login = vi.fn(async () => ({ token: 'user-token', identity: 'user-1' })), command = vi.fn(async () => [])) {
    return { login, command }
  }

  it('logs in with password, flags the account human, and stores the named session for 30 days', async () => {
    // Only the UI flags humans; storing before the flag lands would leave a signed-in human counted as an agent.
    const order: string[] = []
    const session = client(
      vi.fn(async () => {
        order.push('login')
        return { token: 'user-token', identity: 'user-1' }
      }),
      vi.fn(async () => {
        order.push('register')
        return []
      }),
    )
    await guestSession(session, () => NOW).signUp('alice', 'pw')

    expect(session.login).toHaveBeenCalledWith('password', { identifier: 'alice', password: 'pw' })
    expect(session.command).toHaveBeenCalledWith('CALL canvas::register_human()', {}, [])
    expect(order).toEqual(['login', 'register'])
    expect(readStoredSession(SESSION_NAMESPACE)).toEqual({
      token: 'user-token',
      identity: 'user-1',
      walletAddress: 'user-1',
      method: 'password',
      identifier: 'alice',
      expiresAt: NOW / 1000 + SESSION_TTL_SECONDS,
    })
    expect(signedInName()).toBe('alice')
  })

  it('never logs in as a guest', async () => {
    // Guests may not paint, so a guest login would only make an identity that can do nothing.
    const session = client()
    await guestSession(session, () => NOW).signUp('alice', 'pw')
    expect(session.login).toHaveBeenCalledTimes(1)
    expect(session.login).toHaveBeenCalledWith('password', { identifier: 'alice', password: 'pw' })
  })

  it('stores nothing and flags nobody when sign up is refused', async () => {
    // A wrong password for a taken name must leave the visitor a guest, not half signed in.
    const session = client(vi.fn(async () => Promise.reject(new Error('invalid credentials'))))
    await expect(guestSession(session, () => NOW).signUp('alice', 'wrong')).rejects.toThrow('invalid credentials')
    expect(session.command).not.toHaveBeenCalled()
    expect(readStoredSession(SESSION_NAMESPACE)).toBeNull()
    expect(signedInName()).toBeNull()
  })

  it('stores nothing when the human flag is refused', async () => {
    // A stored session without the flag would count this human as an agent until they log in again.
    const session = client(undefined, vi.fn(async () => Promise.reject(new Error('POLICY_001'))))
    await expect(guestSession(session, () => NOW).signUp('alice', 'pw')).rejects.toThrow('POLICY_001')
    expect(readStoredSession(SESSION_NAMESPACE)).toBeNull()
  })
})
