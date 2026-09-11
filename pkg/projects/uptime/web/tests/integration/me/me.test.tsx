// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Component, type ReactNode } from 'react'
import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { ReifyError, Store, type StoreClient } from '@reifydb/react'
import type { TestDb, TestFactory } from '@reifydb/reifydb'
import { AppLayout } from '@/components/layout/app-layout'
import { useMe } from '@/hooks/use-me'
import { connect } from '@/store/client'
import { loadBackend } from '../../support/backend'
import { createUserWithEmail, guestStore, identityNamed, storeAs } from '../../support/identity'
import { setSessionToken, signOut } from '../../support/session-mock'

vi.mock('@reifydb/auth', async () => (await import('../../support/session-mock')).sessionAuthMock())
vi.mock('@tanstack/react-router', async () => ({
  ...(await import('../../support/router-mock')).routerMock(),
  useLocation: () => ({ pathname: '/monitors' }),
  Outlet: () => <MeProbe />,
}))
vi.mock('@/store/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/store/client')>()),
  connect: vi.fn(),
  disconnect: vi.fn(async () => undefined),
}))

function MeProbe() {
  const { data: me } = useMe()
  if (me == null) return <p>me none</p>
  return <p>{`me ${me.id} ${me.email ?? 'no-email'} ${me.guest ? 'guest' : 'user'}`}</p>
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

const GUEST_ID = '01928f00-0000-7000-8000-000000000001'

let create: TestFactory
let db: TestDb
let stores: Map<string, Store>

beforeAll(() => {
  create = loadBackend()
})

beforeEach(() => {
  db = create()
  stores = new Map()
  signOut.mockClear()
  setSessionToken(null)
  vi.mocked(connect).mockImplementation(async (token) => stores.get(token))
})

function renderLayout() {
  return render(<AppLayout />)
}

function refusal(code: string, message: string): ReifyError {
  return new ReifyError({ id: 'req-1', type: 'Err', payload: { diagnostic: { code, message, notes: [] } } })
}

function scriptedStore(query: StoreClient['query']): Store {
  const pending = () => new Promise<never>(() => undefined)
  return new Store({ query, command: pending, admin: pending, subscribe: pending, unsubscribe: async () => undefined })
}

async function settle(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe('me over the store', () => {
  it('shows the signed-in user the query names, with its email', async () => {
    // The navbar must name the caller itself; a me answered for any other identity would show someone else's account.
    const alice = await createUserWithEmail(db, 'alice@example.com')
    await createUserWithEmail(db, 'bob@example.com')
    stores.set('alice-token', storeAs(db, alice))
    setSessionToken('alice-token')

    renderLayout()

    expect(await screen.findByText(`me ${alice} alice@example.com user`)).toBeInTheDocument()
    expect(screen.getByTitle('alice@example.com')).toBeInTheDocument()
    expect(screen.queryByText('You are browsing as a guest')).not.toBeInTheDocument()
  })

  it('marks a guest session and shows the guest banner', async () => {
    // A guest read as a user would lose the sign-up banner and get a sign-out that strands it on the login page.
    stores.set('guest-token', guestStore(GUEST_ID))
    setSessionToken('guest-token')

    renderLayout()

    expect(await screen.findByText(`me ${GUEST_ID} no-email guest`)).toBeInTheDocument()
    expect(screen.getByText('You are browsing as a guest')).toBeInTheDocument()
    expect(screen.getByTitle('Guest session')).toBeInTheDocument()
  })

  it('drops the signed-out identity before the next session reads its own', async () => {
    // A me that outlives sign-out would show the previous account to whoever signs in next in this tab.
    const alice = await createUserWithEmail(db, 'alice@example.com')
    const bob = await createUserWithEmail(db, 'bob@example.com')
    stores.set('alice-token', storeAs(db, alice))
    stores.set('bob-token', storeAs(db, bob))
    setSessionToken('alice-token')
    renderLayout()
    expect(await screen.findByTitle('alice@example.com')).toBeInTheDocument()

    await act(async () => {
      await signOut()
    })

    expect(screen.queryByTitle('alice@example.com')).not.toBeInTheDocument()
    expect(screen.getByTitle('Signed in')).toBeInTheDocument()

    act(() => setSessionToken('bob-token'))

    expect(await screen.findByText(`me ${bob} bob@example.com user`)).toBeInTheDocument()
    expect(screen.queryByTitle('alice@example.com')).not.toBeInTheDocument()
  })

  it('reads a user whose email attribute is none as a signed-in user named by its identity name', async () => {
    // Uptime names a user by its email, so me must take the email from the name; the email attribute may be none.
    await db.adminRoot('CREATE USER plain', {}, [])
    const plain = await identityNamed(db, 'plain')
    stores.set('plain-token', storeAs(db, plain))
    setSessionToken('plain-token')

    renderLayout()

    expect(await screen.findByText(`me ${plain} plain user`)).toBeInTheDocument()
    expect(screen.getByTitle('plain')).toBeInTheDocument()
    expect(screen.queryByText('You are browsing as a guest')).not.toBeInTheDocument()
  })
})

describe('me when its query fails', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('retries a query cut off by a dropped connection, then shows me', async () => {
    // A drop right after connect must not leave the navbar without an account for the rest of the session.
    const query = vi
      .fn()
      .mockRejectedValueOnce(refusal('CONNECTION_LOST', 'Connection lost'))
      .mockResolvedValue([[{ id: 'user-1', name: 'user@example.com', kind: 'user' }]])
    stores.set('user-token', scriptedStore(query))
    setSessionToken('user-token')

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)
    expect(screen.getByText('me none')).toBeInTheDocument()

    await settle(1000)

    expect(screen.getByText('me user-1 user@example.com user')).toBeInTheDocument()
    expect(screen.getByTitle('user@example.com')).toBeInTheDocument()
    expect(query).toHaveBeenCalledTimes(2)
    expect(screen.queryByText(/^crashed:/)).not.toBeInTheDocument()
  })

  it('surfaces a drop that outlasts exactly three retries', async () => {
    // Retries must stop at three; otherwise an outage leaves the navbar without an account forever, with nothing saying why.
    const query = vi.fn().mockRejectedValue(refusal('CONNECTION_LOST', 'Connection lost'))
    stores.set('user-token', scriptedStore(query))
    setSessionToken('user-token')

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)
    await settle(1000)
    await settle(2000)
    expect(query).toHaveBeenCalledTimes(3)
    expect(screen.queryByText(/^crashed:/)).not.toBeInTheDocument()

    await settle(4000)

    expect(screen.getByText('crashed: [CONNECTION_LOST] Connection lost')).toBeInTheDocument()
    await settle(30_000)
    expect(query).toHaveBeenCalledTimes(4)
  })

  it('surfaces any other failure at once, without retrying it', async () => {
    // Only a drop may be retried; otherwise a server error hides behind the backoff and the page shows no account.
    const query = vi.fn().mockRejectedValue(refusal('ASSERT', 'identity has no name'))
    stores.set('user-token', scriptedStore(query))
    setSessionToken('user-token')

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)

    expect(screen.getByText('crashed: [ASSERT] identity has no name')).toBeInTheDocument()
    await settle(30_000)
    expect(query).toHaveBeenCalledTimes(1)
  })

  it('leaves an auth refusal to the sign-out instead of crashing the page', async () => {
    // An auth refusal must only sign out; crashing too would race the sign-out and strand the user on an error page.
    const query = vi.fn().mockRejectedValue(refusal('AUTH_REQUIRED', 'authentication required'))
    stores.set('user-token', scriptedStore(query))
    setSessionToken('user-token')

    render(<Boundary><AppLayout /></Boundary>)
    await settle(0)
    await settle(30_000)

    expect(screen.queryByText(/^crashed:/)).not.toBeInTheDocument()
    expect(screen.getByText('me none')).toBeInTheDocument()
    expect(query).toHaveBeenCalledTimes(1)
  })
})
