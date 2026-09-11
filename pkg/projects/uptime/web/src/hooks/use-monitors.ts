// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { DurationValue, Option, Utf8Value, Uuid7Value, useCommand } from '@reifydb/react'
import { useMonitorRegions } from '@/store/realtime'
import { recorded } from '@/lib/errors'
import type { MonitorInput } from '@/lib/types'

const MONITOR_PARAMS =
  '$name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled'

const CREATE_MONITOR = `CALL uptime::create_monitor($id, ${MONITOR_PARAMS})`

const UPDATE_MONITOR = `CALL uptime::update_monitor($id, ${MONITOR_PARAMS})`

const DELETE_MONITOR = 'CALL uptime::delete_monitor($id)'

const CHECK_MONITOR_REGIONS = 'CALL uptime::check_monitor_regions($id)'

const NO_FRAMES = [] as const

function optionalUtf8(value: string | undefined) {
  return value === undefined ? Option.none('Utf8') : Option.some(new Utf8Value(value))
}

function monitorParams(id: Uuid7Value, input: MonitorInput) {
  return {
    id,
    name: new Utf8Value(input.name),
    kind: new Utf8Value(input.kind),
    target: new Utf8Value(input.target),
    interval: DurationValue.fromMilliseconds(input.interval_ms),
    timeout: DurationValue.fromMilliseconds(input.timeout_ms),
    http_method: optionalUtf8(input.http_method),
    expected_status: input.expected_status === undefined ? Option.none('Int2') : Option.some(input.expected_status),
    keyword: optionalUtf8(input.keyword),
    expected_ip: optionalUtf8(input.expected_ip),
    failure_threshold: input.failure_threshold,
    enabled: input.enabled,
  }
}

function regionCalls(procedure: 'add_monitor_region' | 'remove_monitor_region', key: string, regionIds: string[]) {
  return {
    statements: regionIds.map((_, i) => `CALL uptime::${procedure}($id, $${key}_${i})`),
    params: Object.fromEntries(regionIds.map((regionId, i) => [`${key}_${i}`, new Uuid7Value(regionId)])),
  }
}

export function useCreateMonitor() {
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const create = useCallback(
    async (input: MonitorInput): Promise<string> => {
      const id = Uuid7Value.generate()
      const added = regionCalls('add_monitor_region', 'add', input.regions)
      await recorded(
        run([CREATE_MONITOR, ...added.statements, CHECK_MONITOR_REGIONS].join('; '), {
          ...monitorParams(id, input),
          ...added.params,
        }),
      )
      return id.toString()
    },
    [run],
  )
  return { create, isPending, error }
}

export function useUpdateMonitor(id: string) {
  const { data: current } = useMonitorRegions(id)
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const update = useCallback(
    async (input: MonitorInput): Promise<void> => {
      const before = current.map((mr) => mr.region_id)
      const removed = regionCalls(
        'remove_monitor_region',
        'remove',
        before.filter((regionId) => !input.regions.includes(regionId)),
      )
      const added = regionCalls(
        'add_monitor_region',
        'add',
        input.regions.filter((regionId) => !before.includes(regionId)),
      )
      await recorded(
        run([UPDATE_MONITOR, ...removed.statements, ...added.statements, CHECK_MONITOR_REGIONS].join('; '), {
          ...monitorParams(new Uuid7Value(id), input),
          ...removed.params,
          ...added.params,
        }),
      )
    },
    [run, id, current],
  )
  return { update, isPending, error }
}

export function useDeleteMonitor() {
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const remove = useCallback(
    async (id: string): Promise<void> => {
      await recorded(run(DELETE_MONITOR, { id: new Uuid7Value(id) }))
    },
    [run],
  )
  return { remove, isPending, error }
}
