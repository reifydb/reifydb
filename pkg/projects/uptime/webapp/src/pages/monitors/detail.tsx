// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { Pause, Pencil, Play, Trash2 } from 'lucide-react'
import {
  useDeleteMonitor,
  useMonitor,
  useMonitorResults,
  useUpdateMonitor,
} from '@/hooks/use-monitors'
import type { CheckResult, Monitor } from '@/lib/types'
import { formatDateTime, formatLatency, formatRelativeTime } from '@/lib/format'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { StatusBadge } from '@/components/status/status-badge'
import { UptimeBar } from '@/components/status/uptime-bar'
import { UptimePercent } from '@/components/status/uptime-percent'

function uptime_ratio(results: CheckResult[], window_ms: number): number | null {
  const cutoff = Date.now() - window_ms
  const window = results.filter((r) => new Date(r.checked_at).getTime() >= cutoff)
  if (window.length === 0) return null
  return window.filter((r) => r.success).length / window.length
}

function avg_latency(results: CheckResult[]): number | null {
  const values = results
    .map((r) => r.response_time_ms)
    .filter((v): v is number => v != null)
  if (values.length === 0) return null
  return Math.round(values.reduce((a, b) => a + b, 0) / values.length)
}

function to_input(monitor: Monitor, enabled: boolean) {
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
  }
}

export function MonitorDetailPage() {
  const { monitorId } = useParams({ strict: false }) as { monitorId: string }
  const navigate = useNavigate()
  const { data: monitor, isLoading, error } = useMonitor(monitorId)
  const { data: results } = useMonitorResults(monitorId)
  const update = useUpdateMonitor(monitorId)
  const remove = useDeleteMonitor()

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading...</p>
  if (error != null || monitor == null) {
    return <p className="text-sm text-destructive">Monitor not found</p>
  }

  const checks = results ?? []
  const day_ratio = uptime_ratio(checks, 24 * 3600 * 1000)
  const latency = avg_latency(checks.slice(0, 50))

  function toggle_enabled() {
    if (monitor == null) return
    update.mutate(to_input(monitor, !monitor.enabled))
  }

  function delete_monitor() {
    if (monitor == null) return
    if (!window.confirm(`Delete monitor "${monitor.name}"? This cannot be undone.`)) return
    remove.mutate(monitor.id, {
      onSuccess: () => void navigate({ to: '/dashboard' }),
    })
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold">{monitor.name}</h1>
          <Badge variant="outline">{monitor.kind.toUpperCase()}</Badge>
          {monitor.enabled ? (
            <StatusBadge status={monitor.status} />
          ) : (
            <span className="text-sm text-muted-foreground">Paused</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={toggle_enabled} disabled={update.isPending}>
            {monitor.enabled ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
            {monitor.enabled ? 'Pause' : 'Resume'}
          </Button>
          <Link to="/monitors/$monitorId/edit" params={{ monitorId }}>
            <Button variant="outline" size="sm">
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

      <p className="text-sm text-muted-foreground break-all">{monitor.target}</p>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">Uptime (24h)</CardTitle>
          </CardHeader>
          <CardContent>
            <UptimePercent ratio={day_ratio} className="text-2xl font-semibold" />
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">Avg response (recent)</CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-semibold">{formatLatency(latency)}</span>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">Last check</CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-semibold">
              {formatRelativeTime(monitor.last_checked_at)}
            </span>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm text-muted-foreground">Recent checks</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <UptimeBar results={checks} />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Time</TableHead>
                <TableHead>Result</TableHead>
                <TableHead>Response time</TableHead>
                <TableHead>Detail</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {checks.slice(0, 20).map((r, i) => (
                <TableRow key={`${r.checked_at}-${i}`}>
                  <TableCell className="text-muted-foreground">
                    {formatDateTime(r.checked_at)}
                  </TableCell>
                  <TableCell>
                    <span className={r.success ? 'text-emerald-600' : 'text-red-600'}>
                      {r.success ? 'Up' : 'Down'}
                    </span>
                  </TableCell>
                  <TableCell>{formatLatency(r.response_time_ms)}</TableCell>
                  <TableCell className="text-muted-foreground max-w-72 truncate">
                    {r.error ?? (r.status_code != null ? `HTTP ${r.status_code}` : '-')}
                  </TableCell>
                </TableRow>
              ))}
              {checks.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4} className="text-center text-muted-foreground py-6">
                    No checks recorded yet
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}
