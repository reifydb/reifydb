// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { writeStoredSession } from '@reifydb/auth'
import { Account } from '@/components/account'
import { SESSION_NAMESPACE } from '@/session'

function guest(signUp = vi.fn(async () => undefined)) {
  return { ensure: vi.fn(async () => undefined), signUp }
}

beforeEach(() => {
  localStorage.clear()
})

describe('Account', () => {
  it('signs up with the typed name and password and then shows the name', async () => {
    // The form is the only way a human turns a guest into a named painter.
    const session = guest()
    render(<Account guest={session} />)

    await userEvent.click(screen.getByRole('button', { name: 'Sign up' }))
    await userEvent.type(screen.getByPlaceholderText('Name'), '  alice ')
    await userEvent.type(screen.getByPlaceholderText('Password'), 'pw')
    await userEvent.click(screen.getByRole('button', { name: 'Sign up' }))

    expect(session.signUp).toHaveBeenCalledWith('alice', 'pw')
    expect(await screen.findByText('alice')).toBeInTheDocument()
  })

  it('keeps the form open with the reason when sign up fails', async () => {
    // A refused sign up must say why, or the user retries blindly with the same taken name.
    const session = guest(vi.fn(async () => Promise.reject(new Error('invalid credentials'))))
    render(<Account guest={session} />)

    await userEvent.click(screen.getByRole('button', { name: 'Sign up' }))
    await userEvent.type(screen.getByPlaceholderText('Name'), 'alice')
    await userEvent.type(screen.getByPlaceholderText('Password'), 'pw')
    await userEvent.click(screen.getByRole('button', { name: 'Sign up' }))

    expect(await screen.findByText('Sign up failed: invalid credentials')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('Name')).toBeInTheDocument()
  })

  it('does not allow an empty name or password', async () => {
    // The engine accepts any non-empty password, so empty fields are the only thing the form must stop.
    render(<Account guest={guest()} />)
    await userEvent.click(screen.getByRole('button', { name: 'Sign up' }))
    expect(screen.getByRole('button', { name: 'Sign up' })).toBeDisabled()
    await userEvent.type(screen.getByPlaceholderText('Name'), 'alice')
    expect(screen.getByRole('button', { name: 'Sign up' })).toBeDisabled()
    await userEvent.type(screen.getByPlaceholderText('Password'), 'x')
    expect(screen.getByRole('button', { name: 'Sign up' })).toBeEnabled()
  })

  it('shows the stored name of a returning user instead of the button', () => {
    // A user who signed up before must see they are painting under their name.
    writeStoredSession(SESSION_NAMESPACE, {
      token: 't',
      identity: 'u',
      walletAddress: 'u',
      method: 'password',
      identifier: 'alice',
      expiresAt: Date.now() / 1000 + 60,
    })
    render(<Account guest={guest()} />)
    expect(screen.getByText('alice')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Sign up' })).not.toBeInTheDocument()
  })
})
