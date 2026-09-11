// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Store } from '@reifydb/react'
import type { TestDb, TestFactory } from '@reifydb/reifydb'
import { AuthLayout } from '@/components/layout/auth-layout'
import { useMe } from '@/hooks/use-me'
import { LoginPage } from '@/pages/login'
import { RegisterPage } from '@/pages/register'
import { connect } from '@/store/client'
import { loadBackend } from '../../support/backend'
import { createUserWithEmail, guestStore, storeAs } from '../../support/identity'
import { navigate } from '../../support/router-mock'
import { setSessionToken, signIn } from '../../support/session-mock'

vi.mock('@reifydb/auth', async () => (await import('../../support/session-mock')).sessionAuthMock())
vi.mock('@tanstack/react-router', async () => ({
  ...(await import('../../support/router-mock')).routerMock(),
  Outlet: () => <CurrentPage />,
}))
vi.mock('@/store/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/store/client')>()),
  connect: vi.fn(),
  disconnect: vi.fn(async () => undefined),
}))

let page: 'login' | 'register' = 'login'

function CurrentPage() {
  const { data: me } = useMe()
  return (
    <>
      {page === 'login' ? <LoginPage /> : <RegisterPage />}
      <p>{me == null ? 'me none' : `me ${me.guest ? 'guest' : 'user'}`}</p>
    </>
  )
}

const GUEST_ID = '01928f00-0000-7000-8000-000000000002'
const PASSWORD = 'correct horse battery'

const fetchMock = vi.fn()

let create: TestFactory
let db: TestDb
let stores: Map<string, Store>

beforeAll(() => {
  create = loadBackend()
})

beforeEach(async () => {
  db = create()
  stores = new Map()
  stores.set('guest-token', guestStore(GUEST_ID))
  stores.set('user-token', storeAs(db, await createUserWithEmail(db, 'alice@example.com')))
  navigate.mockClear()
  signIn.mockReset()
  signIn.mockImplementation(async () => setSessionToken('user-token'))
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
  setSessionToken('guest-token')
  vi.mocked(connect).mockReset()
  vi.mocked(connect).mockImplementation(async (token) => stores.get(token))
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function renderAuthPages() {
  return render(<AuthLayout />)
}

describe('me on the auth pages', () => {
  it('keeps a guest on the login page, then moves on once the new session reads as a user', async () => {
    // A guest must be able to reach sign-in, and the page may only leave once me for the signed-in session says user.
    page = 'login'
    const user = userEvent.setup()
    renderAuthPages()
    expect(await screen.findByText('me guest')).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()

    await user.type(screen.getByLabelText('Email'), 'alice@example.com')
    await user.type(screen.getByLabelText('Password'), PASSWORD)
    await user.click(screen.getByRole('button', { name: 'Sign in' }))

    expect(await screen.findByText('me user')).toBeInTheDocument()
    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/monitors' }))
    expect(signIn).toHaveBeenCalledWith({ identifier: 'alice@example.com', password: PASSWORD })
  })

  it('shows the guest hint, then leaves once the promoted session reads as a user', async () => {
    // Registration promotes the guest and signs in again; a me still read from the guest session would strand the page here.
    page = 'register'
    fetchMock.mockResolvedValue({ status: 201, ok: true, text: async () => '' })
    const user = userEvent.setup()
    renderAuthPages()
    expect(await screen.findByText(/this account takes them over/)).toBeInTheDocument()

    await user.type(screen.getByLabelText('Email'), 'alice@example.com')
    await user.type(screen.getByLabelText('Password'), PASSWORD)
    await user.type(screen.getByLabelText('Confirm password'), PASSWORD)
    await user.click(screen.getByRole('button', { name: 'Create account' }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/monitors' }))
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/auth/register',
      expect.objectContaining({ method: 'POST', headers: expect.objectContaining({ Authorization: 'Bearer guest-token' }) }),
    )
    expect(screen.queryByText(/this account takes them over/)).not.toBeInTheDocument()
  })

  it('reads no identity when there is no session', async () => {
    // A signed-out visitor has no connection, so the page must render without one instead of reusing an old me.
    page = 'login'
    setSessionToken(null)

    renderAuthPages()

    expect(await screen.findByRole('button', { name: 'Sign in' })).toBeInTheDocument()
    expect(screen.getByText('me none')).toBeInTheDocument()
    expect(connect).not.toHaveBeenCalled()
    expect(navigate).not.toHaveBeenCalled()
  })
})
