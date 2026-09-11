// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useRef } from 'react'
import { useAuth } from '@reifydb/auth'
import { ApiError, apiFetch, type ApiRequestOptions } from '@/lib/api'
import { useSessionReset } from './use-sign-out'

export type ApiClient = <T>(path: string, opts?: ApiRequestOptions) => Promise<T>

export function useApi(): ApiClient {
  const { session } = useAuth()
  const resetSession = useSessionReset()
  const resetSessionRef = useRef(resetSession)
  const token = session?.token

  useEffect(() => {
    resetSessionRef.current = resetSession
  }, [resetSession])

  return useCallback(
    async <T,>(path: string, opts: ApiRequestOptions = {}): Promise<T> => {
      try {
        return await apiFetch<T>(path, { ...opts, token })
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          await resetSessionRef.current()
        }
        throw err
      }
    },
    [token],
  )
}
