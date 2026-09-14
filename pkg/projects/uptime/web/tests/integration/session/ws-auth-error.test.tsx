// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Component, type ReactNode } from 'react'
import { act, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Client, ReifyError, useSubscription, Uuid7Value, type WsClient } from '@reifydb/react'
import { SessionGate } from '@/components/auth/session-gate'
import { AppLayout } from '@/components/layout/app-layout'
import { clearSignedOut, isSignedOut, markExpiredUserSignedOut, markSignedOut } from '@/lib/session-flags'
import { monitors, regions } from '@/store/queries'
import { navigate } from '../../support/router-mock'
import { adoptSession, setSessionToken, signOut } from '../../support/session-mock'

vi.mock('@reifydb/auth', async () => ({
  storageKeyFor: (await vi.importActual<typeof import('@reifydb/auth')>('@reifydb/auth')).storageKeyFor,
  ...(await import('../../support/session-mock')).sessionAuthMock(),
}))
vi.mock('@tanstack/react-router', async () => ({
  ...(await import('../../support/router-mock')).routerMock(),
  useLocation: () => ({ pathname: '/monitors' }),
  Navigate: ({ to }: { to: string }) => <p>{`redirected to ${to}`}</p>,
  Outlet: () => (
    <>
      <MonitorsProbe />
      <RegionsProbe />
    </>
  ),
}))

function MonitorsProbe() {
  const entry = useSubscription(monitors.rql, null, monitors.shape, { config: monitors.config })
  return <p>monitors {entry.status}</p>
}

function RegionsProbe() {
  const entry = useSubscription(regions.rql, null, regions.shape, { config: regions.config })
  return (
    <>
      <p>regions {entry.status}</p>
      <p>region labels {entry.data.map((region) => region.label).join(',')}</p>
    </>
  )
}

function refusal(code: string, message: string): ReifyError {
  return new ReifyError({ id: 'req-1', type: 'Err', payload: { diagnostic: { code, message, notes: [] } } })
}

function socketAnswering(code: string, message: string) {
  const reply = (): Promise<unknown> => Promise.reject(refusal(code, message))
  return {
    query: vi.fn(reply),
    command: vi.fn(reply),
    admin: vi.fn(reply),
    subscribe: vi.fn(reply),
    unsubscribe: vi.fn(reply),
    batchSubscribe: vi.fn(reply),
    disconnect: vi.fn(async () => undefined),
  }
}

function renderLayout() {
  return render(<AppLayout />)
}

class Boundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state: { error: Error | null } = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  render() {
    return this.state.error == null ? this.props.children : <p>{`crashed: ${this.state.error.message}`}</p>
  }
}

async function settle(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  signOut.mockClear()
  adoptSession.mockClear()
  navigate.mockClear()
  clearSignedOut()
  setSessionToken('rejected-token')
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('a websocket auth error', () => {
  it('resets the session when the server rejects the session token', async () => {
    // A token the server stops accepting must sign out and drop me; otherwise the refused account stays on screen.
    const socket = socketAnswering('AUTH_REQUIRED', 'Authentication required')
    socket.query.mockResolvedValue([[{ id: 'user-1', name: 'user@example.com', kind: 'user' }]])
    let refuse: (err: Error) => void = () => undefined
    socket.batchSubscribe.mockImplementation(
      () =>
        new Promise<unknown>((_, reject) => {
          refuse = reject
        }),
    )
    vi.spyOn(Client, 'connectWs').mockResolvedValue(socket as unknown as WsClient)

    renderLayout()
    expect(await screen.findByTitle('user@example.com')).toBeInTheDocument()

    act(() => refuse(refusal('AUTH_REQUIRED', 'Authentication required')))

    await waitFor(() => expect(signOut).toHaveBeenCalledTimes(1))
    expect(Client.connectWs).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ token: 'rejected-token' }),
    )
    expect(socket.batchSubscribe).toHaveBeenCalled()
    expect(await screen.findByTitle('Signed in')).toBeInTheDocument()
    expect(screen.queryByTitle('user@example.com')).not.toBeInTheDocument()
  })

  it('keeps the session when a request fails because the connection dropped', async () => {
    // A drop rejects in-flight requests with CONNECTION_LOST; signing out on it would log users out on every network blip.
    const socket = socketAnswering('CONNECTION_LOST', 'Connection lost')
    vi.spyOn(Client, 'connectWs').mockResolvedValue(socket as unknown as WsClient)

    renderLayout()

    expect(await screen.findByText('monitors error')).toBeInTheDocument()
    expect(socket.batchSubscribe).toHaveBeenCalled()
    expect(signOut).not.toHaveBeenCalled()
    expect(socket.disconnect).not.toHaveBeenCalled()
  })
})

function refusedAfterMe(me: { id: string; name: string; kind: string }) {
  const socket = socketAnswering('AUTH_REQUIRED', 'Authentication required')
  socket.query.mockResolvedValue([[me]])
  let refuse: (err: Error) => void = () => undefined
  socket.batchSubscribe.mockImplementation(
    () =>
      new Promise<unknown>((_, reject) => {
        refuse = reject
      }),
  )
  return { socket, refuse: () => act(() => refuse(refusal('AUTH_REQUIRED', 'Authentication required'))) }
}

function pendingSocket(me: { id: string; name: string; kind: string }) {
  const socket = socketAnswering('AUTH_REQUIRED', 'Authentication required')
  socket.query.mockResolvedValue([[me]])
  socket.batchSubscribe.mockImplementation(() => new Promise<unknown>(() => undefined))
  return socket
}

function guestEndpoint() {
  const body = { token: 'fresh-guest-token', identity: 'guest-2', expires_at: 4102444800 }
  const fetch = vi.fn(async () => ({ status: 200, ok: true, text: async () => JSON.stringify(body) }))
  vi.stubGlobal('fetch', fetch)
  return fetch
}

function renderGatedLayout() {
  return render(
    <SessionGate>
      <AppLayout />
    </SessionGate>,
  )
}

describe('a websocket auth error behind the session gate', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('sends a signed-in user whose token is refused to the login page without minting a guest', async () => {
    // A refused user token must end at the login page; otherwise the gate mints a guest and the user lands in an empty account.
    const { socket, refuse } = refusedAfterMe({ id: 'user-1', name: 'user@example.com', kind: 'user' })
    vi.spyOn(Client, 'connectWs').mockResolvedValue(socket as unknown as WsClient)
    const fetch = guestEndpoint()
    setSessionToken('user-token')

    renderGatedLayout()
    expect(await screen.findByTitle('user@example.com')).toBeInTheDocument()

    refuse()

    expect(await screen.findByText('redirected to /login')).toBeInTheDocument()
    expect(navigate).toHaveBeenCalledWith({ to: '/login' })
    expect(signOut).toHaveBeenCalledTimes(1)
    expect(isSignedOut()).toBe(true)
    expect(fetch).not.toHaveBeenCalled()
    expect(adoptSession).not.toHaveBeenCalled()
    expect(Client.connectWs).toHaveBeenCalledTimes(1)
  })

  it('replaces a guest whose token is refused with a fresh guest session', async () => {
    // A guest has no account to sign in to, so a refused guest token must mint a new guest instead of stranding it at the login page.
    const first = refusedAfterMe({ id: 'guest-1', name: 'guest-1', kind: 'guest' })
    const second = pendingSocket({ id: 'guest-2', name: 'guest-2', kind: 'guest' })
    vi.spyOn(Client, 'connectWs')
      .mockResolvedValueOnce(first.socket as unknown as WsClient)
      .mockResolvedValueOnce(second as unknown as WsClient)
    const fetch = guestEndpoint()
    setSessionToken('guest-token')

    renderGatedLayout()
    expect(await screen.findByText('You are browsing as a guest')).toBeInTheDocument()

    first.refuse()

    await waitFor(() =>
      expect(Client.connectWs).toHaveBeenLastCalledWith(
        expect.any(String),
        expect.objectContaining({ token: 'fresh-guest-token' }),
      ),
    )
    expect(fetch).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledWith('/api/auth/guest', expect.objectContaining({ method: 'POST' }))
    expect(adoptSession).toHaveBeenCalledWith(expect.objectContaining({ token: 'fresh-guest-token', method: 'token' }))
    expect(navigate).not.toHaveBeenCalled()
    expect(isSignedOut()).toBe(false)
    expect(await screen.findByText('monitors loading')).toBeInTheDocument()
    expect(screen.queryByText('redirected to /login')).not.toBeInTheDocument()
  })
})

describe('a sign-out in another tab', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('sends this tab to the login page without minting a guest', async () => {
    // A signed-out flag only the signing-out tab can see lets every other tab mint a stray guest.
    const fetch = guestEndpoint()
    setSessionToken(null)
    markSignedOut()
    sessionStorage.clear()

    renderGatedLayout()

    expect(await screen.findByText('redirected to /login')).toBeInTheDocument()
    expect(isSignedOut()).toBe(true)
    expect(fetch).not.toHaveBeenCalled()
    expect(adoptSession).not.toHaveBeenCalled()
  })
})

describe('a reload after the stored session expired', () => {
  function storeSession(method: 'password' | 'token', expiresInSeconds: number) {
    const expiresAt = Math.floor(Date.now() / 1000) + expiresInSeconds
    const session = { token: 'stored-token', identity: 'id-1', walletAddress: 'id-1', expiresAt, method }
    localStorage.setItem('reifydb.uptime.auth', JSON.stringify(session))
  }

  beforeEach(() => {
    setSessionToken(null)
  })

  afterEach(() => {
    localStorage.clear()
    vi.unstubAllGlobals()
  })

  it('sends a user whose stored session expired to the login page without minting a guest', async () => {
    // A user session that lapsed while the tab was closed must end at the login page; a guest minted instead hides the account.
    storeSession('password', -60)
    const fetch = guestEndpoint()

    markExpiredUserSignedOut()
    renderGatedLayout()

    expect(await screen.findByText('redirected to /login')).toBeInTheDocument()
    expect(isSignedOut()).toBe(true)
    expect(fetch).not.toHaveBeenCalled()
    expect(adoptSession).not.toHaveBeenCalled()
  })

  it.each([
    ['a guest whose stored session expired', () => storeSession('token', -60)],
    ['a first visit with no stored session', () => undefined],
  ])('gives %s a fresh guest session', async (_, arrange) => {
    // Only a user has an account to return to; a guest or a new visitor sent to the login page could not use the app at all.
    arrange()
    const fetch = guestEndpoint()
    const socket = pendingSocket({ id: 'guest-2', name: 'guest-2', kind: 'guest' })
    vi.spyOn(Client, 'connectWs').mockResolvedValue(socket as unknown as WsClient)

    markExpiredUserSignedOut()
    renderGatedLayout()

    await waitFor(() =>
      expect(adoptSession).toHaveBeenCalledWith(expect.objectContaining({ token: 'fresh-guest-token', method: 'token' })),
    )
    expect(fetch).toHaveBeenCalledTimes(1)
    expect(isSignedOut()).toBe(false)
    expect(await screen.findByText('You are browsing as a guest')).toBeInTheDocument()
  })

  it('leaves a user whose stored session is still valid signed in', () => {
    // A flag set for a live session outlives it, so any later disconnect in this tab would skip the guest fallback for the login page.
    storeSession('password', 3600)

    markExpiredUserSignedOut()

    expect(isSignedOut()).toBe(false)
  })
})

class UnreachableSocket {
  readyState = 0

  addEventListener(type: string, listener: () => void) {
    if (type === 'error') queueMicrotask(listener)
  }

  removeEventListener() {}

  close() {
    this.readyState = 3
  }
}

describe('the first connect', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    sockets = []
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('retries a server it cannot reach, backing off up to a 30s cap, until it connects', async () => {
    // A server down at page load must be retried with a bounded delay; otherwise the page spins forever or waits minutes after it is back.
    let attempts = 0
    vi.stubGlobal('WebSocket', function WebSocket() {
      attempts++
      if (attempts < 8) return new UnreachableSocket()
      const socket = new ScriptedSocket()
      sockets.push(socket)
      return socket
    })

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)
    expect(attempts).toBe(1)
    for (const [delay, expected] of [[1000, 2], [2000, 3], [4000, 4], [8000, 5], [16_000, 6]]) {
      await settle(delay)
      expect(attempts).toBe(expected)
    }
    await settle(29_999)
    expect(attempts).toBe(6)
    await settle(1)
    expect(attempts).toBe(7)
    expect(screen.queryByText('monitors loading')).not.toBeInTheDocument()

    await settle(30_000)

    expect(attempts).toBe(8)
    expect(screen.getByText('monitors loading')).toBeInTheDocument()
    expect(screen.queryByText(/^crashed:/)).not.toBeInTheDocument()
  })

  it('surfaces a first connect that fails for any reason other than an unreachable server, without retrying', async () => {
    // Only an unreachable server may be retried; otherwise a bad socket URL hides behind an endless spinner.
    let attempts = 0
    vi.stubGlobal('WebSocket', function WebSocket() {
      attempts++
      throw new SyntaxError('The URL is invalid')
    })

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)

    expect(screen.getByText('crashed: The URL is invalid')).toBeInTheDocument()
    await settle(60_000)
    expect(attempts).toBe(1)
  })
})

class ScriptedSocket {
  readyState = 1
  readonly sent: any[] = []
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: ((event: unknown) => void) | null = null
  onclose: (() => void) | null = null
  private readonly closeListeners: (() => void)[] = []

  addEventListener(type: string, listener: () => void) {
    if (type === 'close') this.closeListeners.push(listener)
  }

  removeEventListener() {}

  send(data: string) {
    this.sent.push(JSON.parse(data))
  }

  close() {
    if (this.readyState === 3) return
    this.readyState = 3
    this.onclose?.()
    this.closeListeners.forEach((listener) => listener())
  }

  batches(): any[] {
    return this.sent.filter((message) => message.type === 'BatchSubscribe')
  }

  ackBatch() {
    const [request] = this.batches()
    const subscriptions = request.payload.subscriptions.map((_: unknown, index: number) => ({
      index,
      subscription_id: `server-${index}`,
    }))
    this.receive({ id: request.id, type: 'BatchSubscribed', payload: { batch_id: 'batch-1', subscriptions } })
  }

  refuseBatch(code: string) {
    const [request] = this.batches()
    this.receive({ id: request.id, type: 'Err', payload: { diagnostic: { code, message: code, notes: [] } } })
  }

  subscriptionId(table: string): string {
    const [request] = this.batches()
    return `server-${request.payload.subscriptions.findIndex((subscription: { rql: string }) => subscription.rql.includes(table))}`
  }

  insertRegion(subscriptionId: string, rownum: number, label: string) {
    const frame = {
      op: 1,
      row_numbers: [String(rownum)],
      columns: [
        { name: 'id', type: { id: 'Uuid7' }, payload: [Uuid7Value.generate().toString()] },
        { name: 'label', type: { id: 'Utf8' }, payload: [label] },
      ],
    }
    const body = { frames: [frame] }
    this.receive({ type: 'Change', payload: { subscription_id: subscriptionId, content_type: FRAMES, body } })
  }

  private receive(message: unknown) {
    this.onmessage?.({ data: JSON.stringify(message) })
  }
}

let sockets: ScriptedSocket[] = []

const FRAMES = 'application/vnd.reifydb.frames'

async function firstBatch(view: () => unknown = renderLayout): Promise<ScriptedSocket> {
  view()
  await waitFor(() => expect(sockets[0]?.batches()).toHaveLength(1))
  return sockets[0]
}

async function reconnectedLayout(): Promise<ScriptedSocket> {
  const first = await firstBatch()
  act(() => first.ackBatch())
  await screen.findByText('monitors ready')
  await screen.findByText('regions ready')
  act(() => first.close())
  await waitFor(() => expect(sockets[1]?.batches()).toHaveLength(1))
  return sockets[1]
}

describe('a websocket auth error on a batch subscribe', () => {
  beforeEach(() => {
    // Sign-out must keep the session here, otherwise the layout drops the store before a refused subscription can render its error.
    signOut.mockImplementation(async () => undefined)
    sockets = []
    vi.stubGlobal('WebSocket', function WebSocket() {
      const socket = new ScriptedSocket()
      sockets.push(socket)
      return socket
    })
    const connectWs = Client.connectWs.bind(Client)
    vi.spyOn(Client, 'connectWs').mockImplementation((url, options) =>
      connectWs(url, { ...options, reconnectDelayMs: 0 }),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('resets the session when the resubscribe after a reconnect is refused for auth', async () => {
    // A token that expired while offline fails every resubscribe with AUTH_REQUIRED, so the user must be signed out.
    const socket = await reconnectedLayout()

    act(() => socket.refuseBatch('AUTH_REQUIRED'))

    await waitFor(() => expect(signOut).toHaveBeenCalledTimes(1))
    expect(await screen.findByText('monitors error')).toBeInTheDocument()
  })

  it('keeps the session when the resubscribe is cut off by another drop, and resubscribes with fresh rows next time', async () => {
    // A drop mid-resubscribe must neither sign the user out nor mint a guest, and the next reconnect must replace the stale rows.
    signOut.mockImplementation(async () => setSessionToken(null))
    const fetch = vi.fn()
    vi.stubGlobal('fetch', fetch)
    const first = await firstBatch(renderGatedLayout)
    act(() => first.ackBatch())
    act(() => first.insertRegion(first.subscriptionId('uptime::regions'), 1, 'US East'))
    expect(await screen.findByText('region labels US East')).toBeInTheDocument()
    act(() => first.close())
    await waitFor(() => expect(sockets[1]?.batches()).toHaveLength(1))

    act(() => sockets[1].close())

    await waitFor(() => expect(sockets[2]?.batches()).toHaveLength(1))
    const next = sockets[2]
    expect(next.batches()[0].payload.subscriptions).toEqual(first.batches()[0].payload.subscriptions)
    act(() => next.ackBatch())
    act(() => next.insertRegion(next.subscriptionId('uptime::regions'), 2, 'EU West'))
    expect(await screen.findByText('region labels EU West')).toBeInTheDocument()
    expect(screen.getByText('monitors ready')).toBeInTheDocument()
    expect(screen.getByText('regions ready')).toBeInTheDocument()
    expect(signOut).not.toHaveBeenCalled()
    expect(isSignedOut()).toBe(false)
    expect(fetch).not.toHaveBeenCalled()
    expect(screen.queryByText('redirected to /login')).not.toBeInTheDocument()
  })

  it('signs out exactly once when a first batch of several subscriptions is refused for auth', async () => {
    // One refusal fails every subscription of the batch; a sign-out per subscription would run the sign-out and its redirect twice.
    const socket = await firstBatch()
    expect(socket.batches()[0].payload.subscriptions).toHaveLength(2)

    act(() => socket.refuseBatch('AUTH_REQUIRED'))

    expect(await screen.findByText('monitors error')).toBeInTheDocument()
    expect(await screen.findByText('regions error')).toBeInTheDocument()
    expect(signOut).toHaveBeenCalledTimes(1)
  })

  it('signs out exactly once when a resubscribed batch of several subscriptions is refused for auth', async () => {
    // The client reports a refused resubscribe to each subscription; a sign-out per subscription would run the sign-out twice.
    const socket = await reconnectedLayout()
    expect(socket.batches()[0].payload.subscriptions).toHaveLength(2)

    act(() => socket.refuseBatch('AUTH_REQUIRED'))

    expect(await screen.findByText('monitors error')).toBeInTheDocument()
    expect(await screen.findByText('regions error')).toBeInTheDocument()
    expect(signOut).toHaveBeenCalledTimes(1)
  })
})
