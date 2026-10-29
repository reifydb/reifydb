// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { readStoredSession, writeStoredSession } from '@reifydb/auth'
import { ReifyError } from '@reifydb/client'
import { commitPixels, type Retry } from '@/commit'
import type { PlaceClient } from '@/client'
import { SESSION_NAMESPACE, SESSION_TTL_SECONDS, guestSession, openConnection, signedInName } from '@/session'

const NOW = 1_800_000_000_000

function login(result: Promise<{ token: string; identity: string }> = Promise.resolve({ token: 'guest-token', identity: 'guest-1' })) {
  return { login: vi.fn(() => result) }
}

function refused(code: string): ReifyError {
  return new ReifyError({ id: 'auth-1', type: 'Err', payload: { diagnostic: { code, message: 'InvalidToken', notes: [] } } })
}

const NO_RETRY: Retry = { retries: 0, baseDelayMs: 0, sleep: async () => undefined, random: () => 0 }

beforeEach(() => {
  localStorage.clear()
})

describe('guest session', () => {
  it('logs in as a guest on the first ensure and stores the session for 30 days', async () => {
    // The stored session is what keeps one person one guest across reloads; a wrong method or expiry splits their pixels.
    const client = login()
    await guestSession(client, false, () => NOW).ensure()

    expect(client.login).toHaveBeenCalledWith('guest', {})
    expect(readStoredSession(SESSION_NAMESPACE)).toEqual({
      token: 'guest-token',
      identity: 'guest-1',
      walletAddress: 'guest-1',
      method: 'guest',
      expiresAt: NOW / 1000 + SESSION_TTL_SECONDS,
    })
    expect(SESSION_TTL_SECONDS).toBe(30 * 86400)
  })

  it('logs in only once for commits that race the first login', async () => {
    // Two commits in flight must share one guest, or the second creates another identity and overwrites the first.
    const client = login()
    const session = guestSession(client, false, () => NOW)
    await Promise.all([session.ensure(), session.ensure()])
    await session.ensure()
    expect(client.login).toHaveBeenCalledTimes(1)
  })

  it('never logs in when the connection already carries a session', async () => {
    // A returning guest must keep its identity; a fresh login would orphan every pixel it painted before.
    const client = login()
    await guestSession(client, true).ensure()
    expect(client.login).not.toHaveBeenCalled()
    expect(readStoredSession(SESSION_NAMESPACE)).toBeNull()
  })

  it('lets a failed login be tried again on the next commit', async () => {
    // A failed login must not stick, or the page could never paint again until a reload.
    const client = { login: vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValue({ token: 't', identity: 'g' }) }
    const session = guestSession(client, false, () => NOW)
    await expect(session.ensure()).rejects.toThrow('offline')
    await session.ensure()
    expect(client.login).toHaveBeenCalledTimes(2)
  })
})

describe('open connection', () => {
  it('connects with the stored token and reports the connection as signed in', async () => {
    // Without the token the server treats a returning guest as anonymous, and its next commit is refused.
    writeStoredSession(SESSION_NAMESPACE, { token: 'stored', identity: 'g', walletAddress: 'g', method: 'guest', expiresAt: Date.now() / 1000 + 60 })
    const connect = vi.fn(async (token: string | undefined) => ({ token }))

    const opened = await openConnection(connect)

    expect(connect).toHaveBeenCalledTimes(1)
    expect(connect).toHaveBeenCalledWith('stored')
    expect(opened).toEqual({ client: { token: 'stored' }, signedIn: true })
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
    expect(opened.signedIn).toBe(false)
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
    expect(await openConnection(connect)).toEqual({ client: { token: undefined }, signedIn: false })
  })
})

describe('commit with a guest session', () => {
  it('signs in before the first commit is sent', async () => {
    // Anonymous may not paint, so the commit must leave only after the guest login has finished.
    const order: string[] = []
    const client = { command: vi.fn(async () => order.push('command')) } as unknown as PlaceClient
    const guest = { ensure: vi.fn(async () => void order.push('ensure')) }

    const result = await commitPixels(client, [{ x: 1, y: 1, color: 3 }], NO_RETRY, guest)

    expect(result).toEqual({ ok: true, committed: 1 })
    expect(order).toEqual(['ensure', 'command'])
  })

  it('reports a failed guest login and sends nothing', async () => {
    // A commit sent without a guest would be refused anyway; the user must see the login failure instead.
    const client = { command: vi.fn() } as unknown as PlaceClient
    const guest = { ensure: vi.fn(async () => Promise.reject(new Error('offline'))) }

    const result = await commitPixels(client, [{ x: 1, y: 1, color: 3 }], NO_RETRY, guest)

    expect(result).toEqual({ ok: false, message: 'offline' })
    expect(client.command).not.toHaveBeenCalled()
  })

  it('does not sign in for an empty commit', async () => {
    // Clicking merge with nothing pending must not create a guest.
    const client = { command: vi.fn() } as unknown as PlaceClient
    const guest = { ensure: vi.fn(async () => undefined) }
    await commitPixels(client, [], NO_RETRY, guest)
    expect(guest.ensure).not.toHaveBeenCalled()
  })
})

describe('sign up', () => {
  it('logs in with password on the same socket after any guest login, and stores the named session', async () => {
    // The password login must ride the guest's socket after its login, or the server creates a new user instead of promoting.
    let finishGuest: (value: { token: string; identity: string }) => void = () => undefined
    const calls: string[] = []
    const client = {
      login: vi.fn((method: string) => {
        calls.push(method)
        if (method === 'guest') return new Promise<{ token: string; identity: string }>((resolve) => (finishGuest = resolve))
        return Promise.resolve({ token: 'user-token', identity: 'guest-1' })
      }),
    }
    const session = guestSession(client, false, () => NOW)
    const guestLogin = session.ensure()

    const signing = session.signUp('alice', 'pw')
    await Promise.resolve()
    expect(calls).toEqual(['guest'])
    finishGuest({ token: 'guest-token', identity: 'guest-1' })
    await guestLogin
    await signing

    expect(client.login).toHaveBeenLastCalledWith('password', { identifier: 'alice', password: 'pw' })
    expect(readStoredSession(SESSION_NAMESPACE)).toEqual({
      token: 'user-token',
      identity: 'guest-1',
      walletAddress: 'guest-1',
      method: 'password',
      identifier: 'alice',
      expiresAt: NOW / 1000 + SESSION_TTL_SECONDS,
    })
    expect(signedInName()).toBe('alice')
  })

  it('never makes a guest after signing up', async () => {
    // A named user painting next must keep its name, not fall back to a fresh guest.
    const client = { login: vi.fn(async () => ({ token: 'user-token', identity: 'user-1' })) }
    const session = guestSession(client, false, () => NOW)
    await session.signUp('alice', 'pw')
    await session.ensure()
    expect(client.login).toHaveBeenCalledTimes(1)
  })

  it('keeps the guest session when sign up is refused', async () => {
    // A wrong password for a taken name must not drop the guest and its pixels.
    const client = {
      login: vi.fn(async (method: string) => {
        if (method === 'password') throw new Error('invalid credentials')
        return { token: 'guest-token', identity: 'guest-1' }
      }),
    }
    const session = guestSession(client, false, () => NOW)
    await session.ensure()
    await expect(session.signUp('alice', 'wrong')).rejects.toThrow('invalid credentials')
    expect(readStoredSession(SESSION_NAMESPACE)?.method).toBe('guest')
    expect(signedInName()).toBeNull()
  })
})
