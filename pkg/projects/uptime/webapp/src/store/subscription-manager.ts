// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Client, type WsClient } from '@reifydb/client'
import { UPTIME_CONFIG } from '@/config'
import type { Result, Monitor, MonitorKind, MonitorStatus } from '@/lib/types'
import { useRealtimeStore } from './realtime'

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

let client: WsClient | null = null
let currentToken: string | null = null
let generation = 0
const desiredResults = new Set<string>()
const activeResults = new Set<string>()

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
    checked_at: String(row.checked_at),
    success: bool(row.success),
    response_time_ms: durationMs(row.response_time),
    status_code: num(row.status_code),
    error: text(row.error),
  }
}

function dailyKey(row: Record<string, unknown>): string {
  return `${String(row.monitor_id)}|${String(row.day)}`
}

async function subscribeMonitors(c: WsClient): Promise<void> {
  await c.subscribe(
    'from uptime::monitors',
    undefined,
    undefined,
    {
      on_insert: (rows) => store().upsertMonitors(rows.map(toMonitor)),
      on_update: (rows) => store().upsertMonitors(rows.map(toMonitor)),
      on_remove: (rows) => store().removeMonitors(rows.map((r) => String(r.id))),
    },
    { hydration: { enabled: true, max_rows: 1000 } },
  )
}

async function subscribeDaily(c: WsClient, view: string, isUps: boolean): Promise<void> {
  const apply = (rows: Record<string, unknown>[]) => {
    const s = store()
    for (const row of rows) {
      const key = dailyKey(row)
      const n = num(row.n) ?? 0
      if (isUps) s.setDailyUp(key, n)
      else s.setDailyTotal(key, n)
    }
  }
  await c.subscribe(
    `from uptime::${view}`,
    undefined,
    undefined,
    {
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
    { hydration: { enabled: true, max_rows: 20000 } },
  )
}

async function subscribeResults(c: WsClient, monitorId: string): Promise<void> {
  await c.subscribe(
    'from uptime::results filter { monitor_id == cast($monitor_id, uuid7) } map { checked_at, success, response_time, status_code, error } take 200',
    { monitor_id: monitorId },
    undefined,
    {
      on_insert: (rows) => store().insertResults(monitorId, rows.map(toResult)),
      on_update: (rows) => store().insertResults(monitorId, rows.map(toResult)),
      on_remove: (rows) =>
        store().removeResults(
          monitorId,
          rows.map((r) => String(r.checked_at)),
        ),
    },
    { hydration: { enabled: true, max_rows: 200 } },
  )
  activeResults.add(monitorId)
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
    await subscribeMonitors(c)
    store().setMonitorsReady()
    await subscribeDaily(c, 'daily_totals', false)
    await subscribeDaily(c, 'daily_ups', true)
    for (const monitorId of desiredResults) {
      if (gen !== generation) return
      await subscribeResults(c, monitorId)
    }
    if (gen === generation) store().setStatus('live')
  } catch {
    if (gen === generation) store().setStatus('offline')
  }
}

export async function ensureResultsSubscription(monitorId: string): Promise<void> {
  if (!UUID_RE.test(monitorId)) return
  desiredResults.add(monitorId)
  const c = client
  if (c == null || activeResults.has(monitorId)) return
  try {
    await subscribeResults(c, monitorId)
  } catch {
    desiredResults.delete(monitorId)
  }
}

async function teardown(): Promise<void> {
  const c = client
  client = null
  currentToken = null
  activeResults.clear()
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
  desiredResults.clear()
  await teardown()
}
