// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback } from 'react'
import { DurationValue, ListValue, Option, Utf8Value, Uuid7Value, rql, useCommand } from '@reifydb/react'
import { useMonitorRegions } from '@/store/realtime'
import { recorded } from '@/lib/errors'
import type { MonitorInput } from '@/lib/types'

type MonitorParams = ReturnType<typeof monitorParams>

const CREATE_MONITOR = rql.write([])<MonitorParams & { region_ids: ListValue }>`CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled); CALL uptime::add_monitor_regions($id, $region_ids); CALL uptime::check_monitor_regions($id)`

const UPDATE_MONITOR = rql.write([])<
  MonitorParams & { removed_region_ids: ListValue; added_region_ids: ListValue }
>`CALL uptime::update_monitor($id, $name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled); CALL uptime::remove_monitor_regions($id, $removed_region_ids); CALL uptime::add_monitor_regions($id, $added_region_ids); CALL uptime::check_monitor_regions($id)`

const DELETE_MONITOR = rql.write([])<{ id: Uuid7Value }>`CALL uptime::delete_monitor($id)`

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

export function useCreateMonitor() {
  const { run, isPending, error } = useCommand(CREATE_MONITOR)
  const create = useCallback(
    async (input: MonitorInput): Promise<string> => {
      const id = Uuid7Value.generate()
      await recorded(
        run({
          ...monitorParams(id, input),
          region_ids: new ListValue(input.regions.map((regionId) => new Uuid7Value(regionId)), 'Uuid7'),
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
  const { run, isPending, error } = useCommand(UPDATE_MONITOR)
  const update = useCallback(
    async (input: MonitorInput): Promise<void> => {
      const before = current.map((mr) => mr.region_id)
      const removed = before.filter((regionId) => !input.regions.includes(regionId))
      const added = input.regions.filter((regionId) => !before.includes(regionId))
      await recorded(
        run({
          ...monitorParams(new Uuid7Value(id), input),
          removed_region_ids: new ListValue(removed.map((regionId) => new Uuid7Value(regionId)), 'Uuid7'),
          added_region_ids: new ListValue(added.map((regionId) => new Uuid7Value(regionId)), 'Uuid7'),
        }),
      )
    },
    [run, id, current],
  )
  return { update, isPending, error }
}

export function useDeleteMonitor() {
  const { run, isPending, error } = useCommand(DELETE_MONITOR)
  const remove = useCallback(
    async (id: string): Promise<void> => {
      await recorded(run({ id: new Uuid7Value(id) }))
    },
    [run],
  )
  return { remove, isPending, error }
}
