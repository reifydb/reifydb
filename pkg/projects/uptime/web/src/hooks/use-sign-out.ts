// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import type { Store } from '@reifydb/react'
import { markSignedOut } from '@/lib/session-flags'
import { isGuestSession } from './use-me'

export function useSessionReset(): () => Promise<void> {
  const { signOut } = useAuth()
  return useCallback(async () => {
    await signOut()
  }, [signOut])
}

export function useSignOut(): () => Promise<void> {
  const reset = useSessionReset()
  const navigate = useNavigate()
  return useCallback(async () => {
    markSignedOut()
    await reset()
    await navigate({ to: '/login' })
  }, [reset, navigate])
}

export function useEndSession(): (store: Store) => Promise<void> {
  const reset = useSessionReset()
  const signOut = useSignOut()
  return useCallback((store: Store) => (isGuestSession(store) ? reset() : signOut()), [reset, signOut])
}
