// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMemo } from 'react'
import { useSubscription, type DurationValue, type Option } from '@reifydb/react'
import type {
  DailyUptime,
  Monitor,
  MonitorKind,
  MonitorRegion,
  MonitorStatus,
  Region,
  Result,
} from '@/lib/types'
import {
  dailyTotals,
  dailyUps,
  monitorRegions,
  monitors,
  regions,
  results,
  type DailyRow,
  type MonitorRegionRow,
  type MonitorRow,
  type RegionRow,
  type ResultRow,
} from './queries'

const RESULTS_CAP = 200

function durationMs(v: DurationValue): number | null {
  const ms = v.milliseconds()
  return ms == null ? null : Number(ms)
}

function orNull<T>(v: Option<T>): T | null {
  return v.isNone() ? null : v.unwrap()
}

function isoDay(day: Date): string {
  return day.toISOString().slice(0, 10)
}

function toMonitor(row: MonitorRow): Monitor {
  return {
    id: row.id,
    name: row.name,
    kind: row.kind as MonitorKind,
    target: row.target,
    interval_ms: durationMs(row.interval) ?? 0,
    timeout_ms: durationMs(row.timeout) ?? 0,
    http_method: orNull(row.httpMethod),
    expected_status: orNull(row.expectedStatus),
    keyword: orNull(row.keyword),
    expected_ip: orNull(row.expectedIp),
    failure_threshold: row.failureThreshold,
    enabled: row.enabled,
    status: row.status as MonitorStatus,
    created_at: row.createdAt.toISOString(),
    last_checked_at: orNull(row.lastCheckedAt.map((d) => d.toISOString())),
    consecutive_failures: row.consecutiveFailures,
  }
}

function toResult(row: ResultRow): Result {
  return {
    region_id: row.regionId,
    probe: orNull(row.probe),
    checked_at: row.checkedAt.toISOString(),
    success: row.success,
    response_time_ms: orNull(row.responseTime.map(durationMs)),
    status_code: orNull(row.statusCode),
    error: orNull(row.error),
  }
}

function toMonitorRegion(row: MonitorRegionRow): MonitorRegion {
  return {
    monitor_id: row.monitorId,
    region_id: row.regionId,
    status: row.status as MonitorStatus,
    last_checked_at: orNull(row.lastCheckedAt.map((d) => d.toISOString())),
    consecutive_failures: row.consecutiveFailures,
  }
}

function toRegion(row: RegionRow): Region {
  return { id: row.id, label: row.label }
}

function byRegionId(a: MonitorRegion, b: MonitorRegion): number {
  return a.region_id < b.region_id ? -1 : 1
}

export function useLiveMonitors(): Monitor[] | null {
  const entry = useSubscription(monitors.rql, null, monitors.shape, { config: monitors.config })
  return useMemo(() => {
    if (entry.status !== 'ready') return null
    return entry.data.map(toMonitor).sort((a, b) => (a.created_at < b.created_at ? 1 : -1))
  }, [entry.status, entry.data])
}

export function useLiveMonitor(id: string): { monitor: Monitor | undefined; ready: boolean } {
  const entry = useSubscription(monitors.rql, null, monitors.shape, { config: monitors.config })
  return useMemo(() => {
    const row = entry.data.find((r) => r.id === id)
    return { monitor: row == null ? undefined : toMonitor(row), ready: entry.status === 'ready' }
  }, [entry.status, entry.data, id])
}

export function useLiveResults(id: string): Result[] | undefined {
  const entry = useSubscription(results.rql, null, results.shape, { config: results.config })
  return useMemo(
    () =>
      entry.data
        .filter((r) => r.monitorId === id)
        .map(toResult)
        .sort((a, b) => (a.checked_at < b.checked_at ? 1 : -1))
        .slice(0, RESULTS_CAP),
    [entry.data, id],
  )
}

export function useLiveDaily(): Map<string, DailyUptime[]> {
  const totals = useSubscription(dailyTotals.rql, null, dailyTotals.shape, { config: dailyTotals.config })
  const ups = useSubscription(dailyUps.rql, null, dailyUps.shape, { config: dailyUps.config })
  return useMemo(() => {
    const byMonitor = new Map<string, Map<string, DailyUptime>>()
    const bucket = (row: DailyRow): DailyUptime => {
      const day = isoDay(row.day)
      let days = byMonitor.get(row.monitorId)
      if (days == null) {
        days = new Map()
        byMonitor.set(row.monitorId, days)
      }
      let b = days.get(day)
      if (b == null) {
        b = { day, total: 0, up: 0 }
        days.set(day, b)
      }
      return b
    }
    for (const row of totals.data) bucket(row).total = Number(row.n)
    for (const row of ups.data) bucket(row).up = Number(row.n)
    const daily = new Map<string, DailyUptime[]>()
    for (const [monitorId, days] of byMonitor) {
      daily.set(
        monitorId,
        [...days.values()].sort((a, b) => (a.day < b.day ? -1 : 1)),
      )
    }
    return daily
  }, [totals.data, ups.data])
}

export function useRegions(): Region[] {
  const entry = useSubscription(regions.rql, null, regions.shape, { config: regions.config })
  return useMemo(
    () => entry.data.map(toRegion).sort((a, b) => a.label.localeCompare(b.label)),
    [entry.data],
  )
}

export function useRegionLabels(): Record<string, string> {
  const entry = useSubscription(regions.rql, null, regions.shape, { config: regions.config })
  return useMemo(() => {
    const labels: Record<string, string> = {}
    for (const region of entry.data) labels[region.id] = region.label
    return labels
  }, [entry.data])
}

export function useMonitorRegions(monitorId: string): MonitorRegion[] {
  const entry = useSubscription(monitorRegions.rql, null, monitorRegions.shape, {
    config: monitorRegions.config,
  })
  return useMemo(
    () =>
      entry.data
        .filter((mr) => mr.monitorId === monitorId)
        .map(toMonitorRegion)
        .sort(byRegionId),
    [entry.data, monitorId],
  )
}

export function useAllMonitorRegions(): Map<string, MonitorRegion[]> {
  const entry = useSubscription(monitorRegions.rql, null, monitorRegions.shape, {
    config: monitorRegions.config,
  })
  return useMemo(() => {
    const byMonitor = new Map<string, MonitorRegion[]>()
    for (const row of entry.data) {
      const mr = toMonitorRegion(row)
      const list = byMonitor.get(mr.monitor_id) ?? []
      list.push(mr)
      byMonitor.set(mr.monitor_id, list)
    }
    for (const list of byMonitor.values()) list.sort(byRegionId)
    return byMonitor
  }, [entry.data])
}
