// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Client, type BatchSubscriptionMember, type WsClient } from '@reifydb/client'
import { UPTIME_CONFIG } from '@/config'
import type { Monitor, MonitorKind, MonitorRegion, MonitorStatus, Region, Result } from '@/lib/types'
import { useRealtimeStore } from './realtime'

const RESULTS_HYDRATION_CAP = 2000
const BATCH_LINGER_MS = 300

let client: WsClient | null = null
let currentToken: string | null = null
let generation = 0

function store() {
  return useRealtimeStore.getState()
}

function isNone(v: unknown): boolean {
  return v == null || (v as { type?: string }).type === 'None'
}

function text(v: unknown): string | null {
  return isNone(v) ? null : String(v)
}

function num(v: unknown): number | null {
  if (isNone(v)) return null
  const raw = (v as { valueOf(): unknown }).valueOf()
  return raw == null ? null : Number(raw)
}

function bool(v: unknown): boolean {
  return !isNone(v) && (v as { valueOf(): unknown }).valueOf() === true
}

function durationMs(v: unknown): number | null {
  if (isNone(v)) return null
  const ms = (v as { milliseconds(): bigint | undefined }).milliseconds()
  return ms == null ? null : Number(ms)
}

function toMonitor(row: Record<string, unknown>): Monitor {
  return {
    id: String(row.id),
    name: String(row.name),
    kind: String(row.kind) as MonitorKind,
    target: String(row.target),
    interval_ms: durationMs(row.interval) ?? 0,
    timeout_ms: durationMs(row.timeout) ?? 0,
    http_method: text(row.http_method),
    expected_status: num(row.expected_status),
    keyword: text(row.keyword),
    expected_ip: text(row.expected_ip),
    failure_threshold: num(row.failure_threshold) ?? 0,
    enabled: bool(row.enabled),
    status: String(row.status) as MonitorStatus,
    created_at: String(row.created_at),
    last_checked_at: text(row.last_checked_at),
    consecutive_failures: num(row.consecutive_failures) ?? 0,
  }
}

function toResult(row: Record<string, unknown>): Result {
  return {
    region_id: String(row.region_id),
    checked_at: String(row.checked_at),
    success: bool(row.success),
    response_time_ms: durationMs(row.response_time),
    status_code: num(row.status_code),
    error: text(row.error),
  }
}

function toMonitorRegion(row: Record<string, unknown>): MonitorRegion {
  return {
    monitor_id: String(row.monitor_id),
    region_id: String(row.region_id),
    status: String(row.status) as MonitorStatus,
    last_checked_at: text(row.last_checked_at),
    consecutive_failures: num(row.consecutive_failures) ?? 0,
  }
}

function toRegion(row: Record<string, unknown>): Region {
  return {
    id: String(row.id),
    label: String(row.label),
  }
}

function dailyKey(row: Record<string, unknown>): string {
  return `${String(row.monitor_id)}|${String(row.day)}`
}

function groupByMonitor<T>(
  rows: Record<string, unknown>[],
  mapFn: (row: Record<string, unknown>) => T,
): Map<string, T[]> {
  const grouped = new Map<string, T[]>()
  for (const row of rows) {
    const monitorId = String(row.monitor_id)
    const list = grouped.get(monitorId) ?? []
    list.push(mapFn(row))
    grouped.set(monitorId, list)
  }
  return grouped
}

function monitorsMember(): BatchSubscriptionMember {
  return {
    rql: 'from uptime::monitors',
    callbacks: {
      on_insert: (rows) => store().upsertMonitors(rows.map(toMonitor)),
      on_update: (rows) => store().upsertMonitors(rows.map(toMonitor)),
      on_remove: (rows) => store().removeMonitors(rows.map((r: Record<string, unknown>) => String(r.id))),
    },
    config: { hydration: { enabled: true, max_rows: 1000 }, linger: BATCH_LINGER_MS },
  }
}

function monitorRegionsMember(): BatchSubscriptionMember {
  return {
    rql: 'from uptime::monitor_regions',
    callbacks: {
      on_insert: (rows) => store().upsertMonitorRegions(rows.map(toMonitorRegion)),
      on_update: (rows) => store().upsertMonitorRegions(rows.map(toMonitorRegion)),
      on_remove: (rows) => store().removeMonitorRegions(rows.map(toMonitorRegion)),
    },
    config: { hydration: { enabled: true, max_rows: 5000 }, linger: BATCH_LINGER_MS },
  }
}

function regionsMember(): BatchSubscriptionMember {
  return {
    rql: 'from uptime::regions',
    callbacks: {
      on_insert: (rows) => store().upsertRegions(rows.map(toRegion)),
      on_update: (rows) => store().upsertRegions(rows.map(toRegion)),
      on_remove: (rows) => store().removeRegions(rows.map((r: Record<string, unknown>) => String(r.id))),
    },
    config: { hydration: { enabled: true, max_rows: 1000 }, linger: BATCH_LINGER_MS },
  }
}

function resultsMember(): BatchSubscriptionMember {
  const apply = (rows: Record<string, unknown>[]) => {
    const s = store()
    for (const [monitorId, results] of groupByMonitor(rows, toResult)) {
      s.insertResults(monitorId, results)
    }
  }
  return {
    rql: `from uptime::results map { monitor_id, region_id, checked_at, success, response_time, status_code, error } take ${RESULTS_HYDRATION_CAP}`,
    callbacks: {
      on_insert: apply,
      on_update: apply,
      on_remove: (rows) => {
        const s = store()
        for (const [monitorId, checkedAts] of groupByMonitor(rows, (r) => String(r.checked_at))) {
          s.removeResults(monitorId, checkedAts)
        }
      },
    },
    config: { hydration: { enabled: true, max_rows: RESULTS_HYDRATION_CAP }, linger: BATCH_LINGER_MS },
  }
}

function dailyMember(view: string, isUps: boolean): BatchSubscriptionMember {
  const apply = (rows: Record<string, unknown>[]) => {
    const s = store()
    for (const row of rows) {
      const key = dailyKey(row)
      const n = num(row.n) ?? 0
      if (isUps) s.setDailyUp(key, n)
      else s.setDailyTotal(key, n)
    }
  }
  return {
    rql: `from uptime::${view}`,
    callbacks: {
      on_insert: apply,
      on_update: apply,
      on_remove: (rows) => {
        const s = store()
        for (const row of rows) {
          if (isUps) s.removeDailyUp(dailyKey(row))
          else s.removeDailyTotal(dailyKey(row))
        }
      },
    },
    config: { hydration: { enabled: true, max_rows: 20000 }, linger: BATCH_LINGER_MS },
  }
}

export async function startRealtime(token: string): Promise<void> {
  if (client != null && currentToken === token) return
  const gen = ++generation
  await teardown()
  if (gen !== generation) return
  currentToken = token
  store().setStatus('connecting')
  try {
    const c = await Client.connect_ws(UPTIME_CONFIG.wsUrl(), {
      token,
      max_reconnect_attempts: Number.MAX_SAFE_INTEGER,
      reconnect_delay_ms: 1000,
      on_disconnect: () => {
        if (gen === generation) store().setStatus('reconnecting')
      },
      on_reconnect: () => {
        if (gen === generation) store().setStatus('live')
      },
    })
    if (gen !== generation) {
      void c.disconnect()
      return
    }
    client = c
    await c.batch_subscribe([
      monitorsMember(),
      monitorRegionsMember(),
      regionsMember(),
      resultsMember(),
      dailyMember('daily_totals', false),
      dailyMember('daily_ups', true),
    ])
    store().setMonitorsReady()
    if (gen === generation) store().setStatus('live')
  } catch {
    if (gen === generation) store().setStatus('offline')
  }
}

async function teardown(): Promise<void> {
  const c = client
  client = null
  currentToken = null
  store().reset()
  if (c != null) {
    try {
      await c.disconnect()
    } catch {
      void 0
    }
  }
}

export async function stopRealtime(): Promise<void> {
  generation++
  await teardown()
}
