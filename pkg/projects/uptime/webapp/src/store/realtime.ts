// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMemo } from 'react'
import { create } from 'zustand'
import type { Result, DailyUptime, Monitor, MonitorRegion, Region } from '@/lib/types'

export type ConnectionStatus = 'offline' | 'connecting' | 'live' | 'reconnecting'

export interface DailyBucket {
  total: number
  up: number
}

const RESULTS_CAP = 200

function regionKey(monitorId: string, regionId: string): string {
  return `${monitorId}|${regionId}`
}

interface RealtimeState {
  status: ConnectionStatus
  monitorsReady: boolean
  monitors: Record<string, Monitor>
  monitorRegions: Record<string, MonitorRegion>
  regions: Record<string, Region>
  daily: Record<string, DailyBucket>
  results: Record<string, Result[]>
  setStatus: (status: ConnectionStatus) => void
  setMonitorsReady: () => void
  upsertMonitors: (rows: Monitor[]) => void
  removeMonitors: (ids: string[]) => void
  upsertMonitorRegions: (rows: MonitorRegion[]) => void
  removeMonitorRegions: (rows: MonitorRegion[]) => void
  upsertRegions: (rows: Region[]) => void
  removeRegions: (ids: string[]) => void
  setDailyTotal: (key: string, n: number) => void
  setDailyUp: (key: string, n: number) => void
  removeDailyTotal: (key: string) => void
  removeDailyUp: (key: string) => void
  insertResults: (monitorId: string, rows: Result[]) => void
  removeResults: (monitorId: string, checkedAts: string[]) => void
  reset: () => void
}

export const useRealtimeStore = create<RealtimeState>((set) => ({
  status: 'offline',
  monitorsReady: false,
  monitors: {},
  monitorRegions: {},
  regions: {},
  daily: {},
  results: {},
  setStatus: (status) => set({ status }),
  setMonitorsReady: () => set({ monitorsReady: true }),
  upsertMonitors: (rows) =>
    set((s) => {
      const monitors = { ...s.monitors }
      for (const row of rows) monitors[row.id] = row
      return { monitors }
    }),
  removeMonitors: (ids) =>
    set((s) => {
      const monitors = { ...s.monitors }
      const results = { ...s.results }
      const drop = new Set(ids)
      for (const id of ids) {
        delete monitors[id]
        delete results[id]
      }
      const monitorRegions = Object.fromEntries(
        Object.entries(s.monitorRegions).filter(([, mr]) => !drop.has(mr.monitor_id)),
      )
      return { monitors, results, monitorRegions }
    }),
  upsertMonitorRegions: (rows) =>
    set((s) => {
      const monitorRegions = { ...s.monitorRegions }
      for (const row of rows) monitorRegions[regionKey(row.monitor_id, row.region_id)] = row
      return { monitorRegions }
    }),
  removeMonitorRegions: (rows) =>
    set((s) => {
      const monitorRegions = { ...s.monitorRegions }
      for (const row of rows) delete monitorRegions[regionKey(row.monitor_id, row.region_id)]
      return { monitorRegions }
    }),
  upsertRegions: (rows) =>
    set((s) => {
      const regions = { ...s.regions }
      for (const row of rows) regions[row.id] = row
      return { regions }
    }),
  removeRegions: (ids) =>
    set((s) => {
      const regions = { ...s.regions }
      for (const id of ids) delete regions[id]
      return { regions }
    }),
  setDailyTotal: (key, n) =>
    set((s) => ({
      daily: { ...s.daily, [key]: { total: n, up: s.daily[key]?.up ?? 0 } },
    })),
  setDailyUp: (key, n) =>
    set((s) => ({
      daily: { ...s.daily, [key]: { total: s.daily[key]?.total ?? 0, up: n } },
    })),
  removeDailyTotal: (key) =>
    set((s) => {
      const daily = { ...s.daily }
      delete daily[key]
      return { daily }
    }),
  removeDailyUp: (key) =>
    set((s) => {
      const bucket = s.daily[key]
      if (bucket == null) return s
      return { daily: { ...s.daily, [key]: { ...bucket, up: 0 } } }
    }),
  insertResults: (monitorId, rows) =>
    set((s) => {
      const byCheckedAt = new Map((s.results[monitorId] ?? []).map((r) => [r.checked_at, r]))
      for (const row of rows) byCheckedAt.set(row.checked_at, row)
      const merged = [...byCheckedAt.values()]
        .sort((a, b) => (a.checked_at < b.checked_at ? 1 : -1))
        .slice(0, RESULTS_CAP)
      return { results: { ...s.results, [monitorId]: merged } }
    }),
  removeResults: (monitorId, checkedAts) =>
    set((s) => {
      const existing = s.results[monitorId]
      if (existing == null) return s
      const drop = new Set(checkedAts)
      return {
        results: { ...s.results, [monitorId]: existing.filter((r) => !drop.has(r.checked_at)) },
      }
    }),
  reset: () =>
    set({
      status: 'offline',
      monitorsReady: false,
      monitors: {},
      monitorRegions: {},
      regions: {},
      daily: {},
      results: {},
    }),
}))

export function useConnectionStatus(): ConnectionStatus {
  return useRealtimeStore((s) => s.status)
}

export function useLiveMonitors(): Monitor[] | null {
  const ready = useRealtimeStore((s) => s.monitorsReady)
  const monitors = useRealtimeStore((s) => s.monitors)
  return useMemo(() => {
    if (!ready) return null
    return Object.values(monitors).sort((a, b) => (a.created_at < b.created_at ? 1 : -1))
  }, [ready, monitors])
}

export function useLiveMonitor(id: string): { monitor: Monitor | undefined; ready: boolean } {
  const ready = useRealtimeStore((s) => s.monitorsReady)
  const monitor = useRealtimeStore((s) => s.monitors[id])
  return { monitor, ready }
}

export function useLiveResults(id: string): Result[] | undefined {
  return useRealtimeStore((s) => s.results[id])
}

export function useLiveDaily(): Map<string, DailyUptime[]> {
  const daily = useRealtimeStore((s) => s.daily)
  return useMemo(() => {
    const byMonitor = new Map<string, DailyUptime[]>()
    for (const [key, bucket] of Object.entries(daily)) {
      const sep = key.indexOf('|')
      const monitorId = key.slice(0, sep)
      const day = key.slice(sep + 1)
      const list = byMonitor.get(monitorId) ?? []
      list.push({ day, total: bucket.total, up: bucket.up })
      byMonitor.set(monitorId, list)
    }
    for (const list of byMonitor.values()) {
      list.sort((a, b) => (a.day < b.day ? -1 : 1))
    }
    return byMonitor
  }, [daily])
}

export function useRegions(): Region[] {
  const regions = useRealtimeStore((s) => s.regions)
  return useMemo(() => Object.values(regions).sort((a, b) => a.label.localeCompare(b.label)), [regions])
}

export function useRegionLabels(): Record<string, string> {
  const regions = useRealtimeStore((s) => s.regions)
  return useMemo(() => {
    const labels: Record<string, string> = {}
    for (const region of Object.values(regions)) labels[region.id] = region.label
    return labels
  }, [regions])
}

export function useMonitorRegions(monitorId: string): MonitorRegion[] {
  const monitorRegions = useRealtimeStore((s) => s.monitorRegions)
  return useMemo(
    () =>
      Object.values(monitorRegions)
        .filter((mr) => mr.monitor_id === monitorId)
        .sort((a, b) => (a.region_id < b.region_id ? -1 : 1)),
    [monitorRegions, monitorId],
  )
}

export function useAllMonitorRegions(): Map<string, MonitorRegion[]> {
  const monitorRegions = useRealtimeStore((s) => s.monitorRegions)
  return useMemo(() => {
    const byMonitor = new Map<string, MonitorRegion[]>()
    for (const mr of Object.values(monitorRegions)) {
      const list = byMonitor.get(mr.monitor_id) ?? []
      list.push(mr)
      byMonitor.set(mr.monitor_id, list)
    }
    for (const list of byMonitor.values()) {
      list.sort((a, b) => (a.region_id < b.region_id ? -1 : 1))
    }
    return byMonitor
  }, [monitorRegions])
}
