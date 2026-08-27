// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useApi } from './use-api'
import type { Probe } from '@/lib/types'

export function useProbes() {
  const api = useApi()
  return useQuery({
    queryKey: ['probes'],
    queryFn: () => api<Probe[]>('/probes'),
    refetchInterval: 10_000,
  })
}

export function useProbeNames(): Record<string, string> {
  const { data } = useProbes()
  return useMemo(() => {
    const names: Record<string, string> = {}
    for (const probe of data ?? []) names[probe.id] = probe.name
    return names
  }, [data])
}
