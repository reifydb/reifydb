// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useApi } from './use-api'
import type { CheckResult, Monitor, MonitorInput } from '@/lib/types'

export function useMonitors() {
  const api = useApi()
  return useQuery({
    queryKey: ['monitors'],
    queryFn: () => api<Monitor[]>('/monitors'),
    refetchInterval: 30_000,
  })
}

export function useMonitor(id: string) {
  const api = useApi()
  return useQuery({
    queryKey: ['monitor', id],
    queryFn: () => api<Monitor>(`/monitors/${id}`),
    refetchInterval: 30_000,
  })
}

export function useMonitorResults(id: string) {
  const api = useApi()
  return useQuery({
    queryKey: ['monitor', id, 'results'],
    queryFn: () => api<CheckResult[]>(`/monitors/${id}/results`),
    refetchInterval: 30_000,
  })
}

export function useCreateMonitor() {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: MonitorInput) =>
      api<Monitor>('/monitors', { method: 'POST', body: input }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['monitors'] })
    },
  })
}

export function useUpdateMonitor(id: string) {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: MonitorInput) =>
      api<Monitor>(`/monitors/${id}`, { method: 'PUT', body: input }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['monitors'] })
      void queryClient.invalidateQueries({ queryKey: ['monitor', id] })
    },
  })
}

export function useDeleteMonitor() {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api<void>(`/monitors/${id}`, { method: 'DELETE' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['monitors'] })
    },
  })
}
