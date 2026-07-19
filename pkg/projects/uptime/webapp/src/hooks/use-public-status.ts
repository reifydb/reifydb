// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import type { PublicStatus } from '@/lib/types'

export function usePublicStatus(slug: string) {
  return useQuery({
    queryKey: ['public-status', slug],
    queryFn: () => apiFetch<PublicStatus>(`/public/status/${encodeURIComponent(slug)}`),
    refetchInterval: 60_000,
    retry: (failureCount, error) => {
      if (error instanceof Error && 'status' in error && (error as { status: number }).status === 404) {
        return false
      }
      return failureCount < 3
    },
  })
}
