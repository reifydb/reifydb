// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useQuery } from '@tanstack/react-query'
import { useAuth } from '@reifydb/auth'
import { useApi } from './use-api'
import type { Me } from '@/lib/types'

export function useMe() {
  const api = useApi()
  const { status } = useAuth()
  return useQuery({
    queryKey: ['me'],
    queryFn: () => api<Me>('/me'),
    enabled: status === 'authenticated',
    staleTime: Infinity,
  })
}
