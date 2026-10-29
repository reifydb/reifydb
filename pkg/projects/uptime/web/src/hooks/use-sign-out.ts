// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { markSignedOut } from '@/lib/session-flags'

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

export function useEndSession(): () => Promise<void> {
  const { session } = useAuth()
  const guest = session?.method === 'guest'
  const reset = useSessionReset()
  const signOut = useSignOut()
  return useCallback(() => (guest ? reset() : signOut()), [guest, reset, signOut])
}
