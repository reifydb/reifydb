// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useApi } from '@/hooks/use-api'
import { setSessionToken, signOut } from '../../support/session-mock'

vi.mock('@reifydb/auth', async () => (await import('../../support/session-mock')).sessionAuthMock())

function refusedToken() {
  const body = { error: 'token expired' }
  const fetch = vi.fn(async () => ({ status: 401, ok: false, text: async () => JSON.stringify(body) }))
  vi.stubGlobal('fetch', fetch)
  return fetch
}

beforeEach(() => {
  signOut.mockClear()
  setSessionToken('user-token')
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('an API request refused with 401', () => {
  it('resets the session and still rejects with the API error', async () => {
    // A dead token kept after a 401 would have every later request refused while the page still looks signed in.
    const fetch = refusedToken()
    const { result } = renderHook(() => useApi())

    await act(async () => {
      await expect(result.current('/me')).rejects.toMatchObject({ status: 401, message: 'token expired' })
    })

    expect(signOut).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledWith(
      '/api/me',
      expect.objectContaining({ headers: expect.objectContaining({ Authorization: 'Bearer user-token' }) }),
    )
  })

  it('rejects with the session reset failure instead of dropping it', async () => {
    // A dropped reset rejection is unhandled, so the caller would carry on as if the dead session had been cleared.
    signOut.mockRejectedValueOnce(new Error('logout failed'))
    refusedToken()
    const { result } = renderHook(() => useApi())

    await expect(result.current('/me')).rejects.toThrow('logout failed')

    expect(signOut).toHaveBeenCalledTimes(1)
  })
})
