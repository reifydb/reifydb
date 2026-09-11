// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useSyncExternalStore } from 'react'
import { vi } from 'vitest'

let token: string | null = null
const listeners = new Set<() => void>()

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function setSessionToken(next: string | null): void {
  // Must notify like the real provider's state, otherwise a layout keyed on the token never sees the change.
  token = next
  for (const listener of listeners) listener()
}

export const signOut = vi.fn(async () => {
  // Must clear the session like the real signOut, otherwise a stale me could only be tested against a session that never ends.
  setSessionToken(null)
})

export const signIn = vi.fn(async () => undefined)

export const adoptSession = vi.fn((session: { token: string }) => {
  // Must install the token like the real provider, otherwise a minted guest session never reaches the layout.
  setSessionToken(session.token)
})

export function sessionAuthMock() {
  return {
    readStoredSession: () => null,
    useAuth: () => {
      const current = useSyncExternalStore(subscribe, () => token)
      return {
        session: current == null ? null : { token: current },
        status: current == null ? 'disconnected' : 'authenticated',
        error: null,
        signIn,
        signOut,
        adoptSession,
      }
    },
  }
}
