// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { useMutation } from '@tanstack/react-query'
import { DurationValue, Option, Shape, useCommand } from '@reifydb/react'
import { useApi } from './use-api'
import type { Monitor, MonitorInput } from '@/lib/types'

const CREATE_MONITOR =
  'CALL uptime::create_monitor($name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)'

const CREATED = [Shape.object({ id: Shape.uuid7() })] as const

export function useCreateMonitor() {
  const { run, isPending, error } = useCommand(CREATED)
  const create = useCallback(
    async (input: MonitorInput): Promise<string> => {
      // regions are not sent: RQL has no list-typed procedure parameter yet, so the procedure cannot take them
      const [rows] = await run(CREATE_MONITOR, {
        name: input.name,
        kind: input.kind,
        target: input.target,
        interval: DurationValue.fromMilliseconds(input.interval_ms),
        timeout: DurationValue.fromMilliseconds(input.timeout_ms),
        http_method: input.http_method === undefined ? Option.none('Utf8') : Option.some(input.http_method),
        expected_status: input.expected_status === undefined ? Option.none('Int2') : Option.some(input.expected_status),
        keyword: input.keyword === undefined ? Option.none('Utf8') : Option.some(input.keyword),
        expected_ip: input.expected_ip === undefined ? Option.none('Utf8') : Option.some(input.expected_ip),
        failure_threshold: input.failure_threshold,
        enabled: input.enabled,
      })
      const id = rows[0]?.id
      if (id == null) throw new Error('uptime::create_monitor returned no id')
      return id
    },
    [run],
  )
  return { create, isPending, error }
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
