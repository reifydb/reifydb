// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useApi } from './use-api'
import type { StatusPage, StatusPageInput } from '@/lib/types'

export function useStatusPages() {
  const api = useApi()
  return useQuery({
    queryKey: ['status-pages'],
    queryFn: () => api<StatusPage[]>('/status-pages'),
  })
}

export function useStatusPage(id: string) {
  const api = useApi()
  return useQuery({
    queryKey: ['status-page', id],
    queryFn: () => api<StatusPage>(`/status-pages/${id}`),
  })
}

export function useCreateStatusPage() {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: StatusPageInput) =>
      api<StatusPage>('/status-pages', { method: 'POST', body: input }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['status-pages'] })
    },
  })
}

export function useUpdateStatusPage(id: string) {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: StatusPageInput) =>
      api<StatusPage>(`/status-pages/${id}`, { method: 'PUT', body: input }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['status-pages'] })
      void queryClient.invalidateQueries({ queryKey: ['status-page', id] })
    },
  })
}

export function useDeleteStatusPage() {
  const api = useApi()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) =>
      api<void>(`/status-pages/${id}`, { method: 'DELETE' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['status-pages'] })
    },
  })
}
