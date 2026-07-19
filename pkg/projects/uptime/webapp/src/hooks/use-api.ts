// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { useAuth } from '@reifydb/auth'
import { ApiError, apiFetch, type ApiRequestOptions } from '@/lib/api'

export type ApiClient = <T>(path: string, opts?: ApiRequestOptions) => Promise<T>

export function useApi(): ApiClient {
  const { session, signOut } = useAuth()
  const token = session?.token
  return useCallback(
    async <T,>(path: string, opts: ApiRequestOptions = {}): Promise<T> => {
      try {
        return await apiFetch<T>(path, { ...opts, token })
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          void signOut()
        }
        throw err
      }
    },
    [token, signOut],
  )
}
