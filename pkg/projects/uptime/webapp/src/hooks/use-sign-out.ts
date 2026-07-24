// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useQueryClient } from '@tanstack/react-query'
import { useAuth } from '@reifydb/auth'
import { markSignedOut } from '@/lib/session-flags'

export function useSessionReset(): () => Promise<void> {
  const { signOut } = useAuth()
  const queryClient = useQueryClient()
  return useCallback(async () => {
    await signOut()
    queryClient.clear()
  }, [signOut, queryClient])
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
