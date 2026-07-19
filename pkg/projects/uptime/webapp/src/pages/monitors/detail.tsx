// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type ReactNode } from 'react'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { Pause, Pencil, Play, Trash2 } from 'lucide-react'
import { useDeleteMonitor, useUpdateMonitor } from '@/hooks/use-monitors'
import { useLiveMonitor, useLiveResults, useMonitorRegions, useRegionLabels } from '@/store/realtime'
import type { Result, Monitor } from '@/lib/types'
import { formatDateTime, formatLatency } from '@/lib/format'
import {
  Badge,
  Button,
  Card,
  CardContent,
  EmptyState,
  Loading,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@reifydb/ui'
import { RelativeTime } from '@/components/relative-time'
import { StatusBadge } from '@/components/status/status-badge'
import { StatusDot } from '@/components/status/status-dot'
import { UptimeBar } from '@/components/status/uptime-bar'
import { UptimePercent } from '@/components/status/uptime-percent'

function uptime_ratio(results: Result[], window_ms: number): number | null {
  const cutoff = Date.now() - window_ms
  const window = results.filter((r) => new Date(r.checked_at).getTime() >= cutoff)
  if (window.length === 0) return null
  return window.filter((r) => r.success).length / window.length
}

function avg_latency(results: Result[]): number | null {
  const values = results
    .map((r) => r.response_time_ms)
    .filter((v): v is number => v != null)
  if (values.length === 0) return null
  return Math.round(values.reduce((a, b) => a + b, 0) / values.length)
}

function to_input(monitor: Monitor, regions: string[], enabled: boolean) {
  return {
    name: monitor.name,
    kind: monitor.kind,
    target: monitor.target,
    interval_ms: monitor.interval_ms,
    timeout_ms: monitor.timeout_ms,
    http_method: monitor.http_method ?? undefined,
    expected_status: monitor.expected_status ?? undefined,
    keyword: monitor.keyword ?? undefined,
    expected_ip: monitor.expected_ip ?? undefined,
    failure_threshold: monitor.failure_threshold,
    enabled,
    regions,
  }
}

export function MonitorDetailPage() {
  const { monitorId } = useParams({ strict: false }) as { monitorId: string }
  const navigate = useNavigate()
  const { monitor, ready } = useLiveMonitor(monitorId)
  const results = useLiveResults(monitorId)
  const monitorRegions = useMonitorRegions(monitorId)
  const regionLabels = useRegionLabels()
  const update = useUpdateMonitor(monitorId)
  const remove = useDeleteMonitor()
  const [regionFilter, setRegionFilter] = useState<string | null>(null)

  if (!ready) return <Loading />
  if (monitor == null) {
    return <p className="text-sm text-status-error">Monitor not found</p>
  }

  const checks = results ?? []
  const regionIds = monitorRegions.map((mr) => mr.region_id)
  const upCount = monitorRegions.filter((mr) => mr.status === 'up').length
  const filteredChecks =
    regionFilter == null ? checks : checks.filter((r) => r.region_id === regionFilter)

  function toggle_enabled() {
    if (monitor == null) return
    update.mutate(to_input(monitor, regionIds, !monitor.enabled))
  }

  function delete_monitor() {
    if (monitor == null) return
    if (!window.confirm(`Delete monitor "${monitor.name}"? This cannot be undone.`)) return
    remove.mutate(monitor.id, {
      onSuccess: () => void navigate({ to: '/monitors' }),
    })
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-3 flex-wrap">
          <h1 className="text-2xl">{monitor.name}</h1>
          <Badge
            variant="outline"
            className="border border-border-subtle px-1.5 py-0.5 font-mono text-[10px] uppercase"
          >
            {monitor.kind.toUpperCase()}
          </Badge>
          {monitor.enabled ? (
            <span className="inline-flex items-center gap-2">
              <StatusBadge status={monitor.status} />
              {monitorRegions.length > 0 && (
                <span className="font-mono text-xs text-text-muted">
                  {upCount}/{monitorRegions.length} regions up
                </span>
              )}
            </span>
          ) : (
            <span className="text-sm text-text-muted">Paused</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={toggle_enabled} disabled={update.isPending}>
            {monitor.enabled ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
            {monitor.enabled ? 'Pause' : 'Resume'}
          </Button>
          <Link to="/monitors/$monitorId/edit" params={{ monitorId }}>
            <Button variant="secondary" size="sm">
              <Pencil className="h-4 w-4" />
              Edit
            </Button>
          </Link>
          <Button
            variant="destructive"
            size="sm"
            onClick={delete_monitor}
            disabled={remove.isPending}
          >
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>

      <p className="text-sm text-text-muted break-all">{monitor.target}</p>

      <div className="space-y-3">
        <h2 className="label-uppercase text-xs text-text-muted">
          Checked from {monitorRegions.length}{' '}
          {monitorRegions.length === 1 ? 'region' : 'regions'}
        </h2>
        {monitorRegions.length === 0 ? (
          <Card>
            <EmptyState title="No regions assigned" description="Edit the monitor to add checker regions." />
          </Card>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {monitorRegions.map((mr) => {
              const regionResults = checks.filter((r) => r.region_id === mr.region_id)
              return (
                <div key={mr.region_id} className="glass-card p-4 space-y-3">
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-mono text-xs uppercase tracking-wide text-text-secondary">
                      {regionLabels[mr.region_id] ?? 'Unknown region'}
                    </span>
                    <StatusBadge status={mr.status} className="text-xs" />
                  </div>
                  <div className="flex items-baseline gap-3">
                    <span className="text-lg font-bold">
                      {formatLatency(avg_latency(regionResults.slice(0, 50)))}
                    </span>
                    <UptimePercent
                      ratio={uptime_ratio(regionResults, 24 * 3600 * 1000)}
                      className="text-sm text-text-muted"
                    />
                  </div>
                  <UptimeBar results={regionResults} max={40} />
                  <p className="font-mono text-[11px] text-text-muted">
                    Last <RelativeTime iso={mr.last_checked_at} />
                  </p>
                </div>
              )
            })}
          </div>
        )}
      </div>

      <Card>
        <CardContent className="space-y-4 pt-6">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <h2 className="label-uppercase text-xs text-text-muted">Recent checks</h2>
            {monitorRegions.length > 1 && (
              <div className="flex flex-wrap gap-1.5">
                <FilterChip active={regionFilter == null} onClick={() => setRegionFilter(null)}>
                  All regions
                </FilterChip>
                {monitorRegions.map((mr) => (
                  <FilterChip
                    key={mr.region_id}
                    active={regionFilter === mr.region_id}
                    onClick={() => setRegionFilter(mr.region_id)}
                  >
                    {regionLabels[mr.region_id] ?? 'Unknown'}
                  </FilterChip>
                ))}
              </div>
            )}
          </div>
          {filteredChecks.length === 0 ? (
            <EmptyState title="No checks recorded yet" />
          ) : (
            <Table>
              <TableHead>
                <TableHeader>Time</TableHeader>
                <TableHeader>Region</TableHeader>
                <TableHeader>Result</TableHeader>
                <TableHeader>Response time</TableHeader>
                <TableHeader>Detail</TableHeader>
              </TableHead>
              <TableBody>
                {filteredChecks.slice(0, 30).map((r, i) => (
                  <TableRow key={`${r.checked_at}-${r.region_id}-${i}`}>
                    <TableCell className="text-text-muted">{formatDateTime(r.checked_at)}</TableCell>
                    <TableCell className="font-mono text-xs text-text-secondary">
                      {regionLabels[r.region_id] ?? 'Unknown'}
                    </TableCell>
                    <TableCell>
                      <span className="inline-flex items-center gap-2">
                        <StatusDot status={r.success ? 'up' : 'down'} />
                        <span className={r.success ? 'text-status-success' : 'text-status-error'}>
                          {r.success ? 'Up' : 'Down'}
                        </span>
                      </span>
                    </TableCell>
                    <TableCell>{formatLatency(r.response_time_ms)}</TableCell>
                    <TableCell className="text-text-muted max-w-72 truncate">
                      {r.error ?? (r.status_code != null ? `HTTP ${r.status_code}` : '-')}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function FilterChip({
  active,
  onClick,
  children,
}: {
  active: boolean
  onClick: () => void
  children: ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`border px-2.5 py-1 font-mono text-[11px] uppercase tracking-wide transition-colors ${
        active
          ? 'border-primary bg-primary/10 text-primary-dark'
          : 'border-border-subtle bg-bg-secondary text-text-muted hover:text-primary-dark'
      }`}
    >
      {children}
    </button>
  )
}
