// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMutation } from '@tanstack/react-query'
import { useApi } from './use-api'
import type { Monitor, MonitorInput } from '@/lib/types'

export function useCreateMonitor() {
  const api = useApi()
  return useMutation({
    mutationFn: (input: MonitorInput) =>
      api<Monitor>('/monitors', { method: 'POST', body: input }),
  })
}

export function useUpdateMonitor(id: string) {
  const api = useApi()
  return useMutation({
    mutationFn: (input: MonitorInput) =>
      api<Monitor>(`/monitors/${id}`, { method: 'PUT', body: input }),
  })
}

export function useDeleteMonitor() {
  const api = useApi()
  return useMutation({
    mutationFn: (id: string) => api<void>(`/monitors/${id}`, { method: 'DELETE' }),
  })
}
