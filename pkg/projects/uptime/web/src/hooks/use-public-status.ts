// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'
import { ApiError, apiFetch } from '@/lib/api'
import type { PublicStatus } from '@/lib/types'

const REFETCH_INTERVAL_MS = 60_000
const MAX_RETRIES = 3

interface PublicStatusResult {
  data: PublicStatus | undefined
  error: Error | null
  isLoading: boolean
}

const LOADING: PublicStatusResult = { data: undefined, error: null, isLoading: true }

export function usePublicStatus(slug: string): PublicStatusResult {
  const [result, setResult] = useState<PublicStatusResult>(LOADING)

  useEffect(() => {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    setResult(LOADING)

    const load = async (attempt: number): Promise<void> => {
      try {
        const data = await apiFetch<PublicStatus>(`/public/status/${encodeURIComponent(slug)}`, {
          signal: controller.signal,
        })
        if (controller.signal.aborted) return
        setResult({ data, error: null, isLoading: false })
      } catch (err) {
        if (controller.signal.aborted) return
        const notFound = err instanceof ApiError && err.status === 404
        if (!notFound && attempt < MAX_RETRIES) {
          timer = setTimeout(() => void load(attempt + 1), 1000 * 2 ** attempt)
          return
        }
        const error = err instanceof Error ? err : new Error(String(err))
        setResult((prev) => ({ data: prev.data, error, isLoading: false }))
        if (notFound) return
      }
      timer = setTimeout(() => void load(0), REFETCH_INTERVAL_MS)
    }

    void load(0)
    return () => {
      controller.abort()
      clearTimeout(timer)
    }
  }, [slug])

  return result
}
