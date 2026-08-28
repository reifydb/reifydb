// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Fragment, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { ChevronRight, Plus } from 'lucide-react'
import {
  useAllMonitorRegions,
  useLiveDaily,
  useLiveMonitors,
  useLiveResults,
  useRegionLabels,
} from '@/store/realtime'
import type { MonitorRegion, Result } from '@/lib/types'
import { formatLatency } from '@/lib/format'
import { RelativeTime } from '@/components/relative-time'
import {
  Badge,
  Button,
  Card,
  EmptyState,
  Loading,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@reifydb/ui'
import { DailyUptimeBar } from '@/components/status/daily-uptime-bar'
import { StatusBadge } from '@/components/status/status-badge'
import { StatusDot } from '@/components/status/status-dot'
import { UptimeBar } from '@/components/status/uptime-bar'

function avg_latency(results: Result[]): number | null {
  const values = results.map((r) => r.response_time_ms).filter((v): v is number => v != null)
  if (values.length === 0) return null
  return Math.round(values.reduce((a, b) => a + b, 0) / values.length)
}

function RegionRows({
  monitorId,
  regions,
  labels,
}: {
  monitorId: string
  regions: MonitorRegion[]
  labels: Record<string, string>
}) {
  const results = useLiveResults(monitorId) ?? []
  return (
    <>
      {regions.map((r) => {
        const regionResults = results.filter((res) => res.region_id === r.region_id)
        return (
          <TableRow key={r.region_id} className="bg-bg-secondary/40">
            <TableCell className="pl-10 font-mono text-xs text-text-secondary">
              {labels[r.region_id] ?? 'Unknown region'}
            </TableCell>
            <TableCell>{''}</TableCell>
            <TableCell>
              <span className="inline-flex items-center gap-2 font-mono text-xs">
                <StatusDot status={r.status} />
                {formatLatency(avg_latency(regionResults.slice(0, 20)))}
              </span>
            </TableCell>
            <TableCell className="min-w-48">
              <UptimeBar results={regionResults} max={40} />
            </TableCell>
            <TableCell>{''}</TableCell>
            <TableCell className="text-text-muted">
              <RelativeTime iso={r.last_checked_at} />
            </TableCell>
          </TableRow>
        )
      })}
    </>
  )
}

export function DashboardPage() {
  const monitors = useLiveMonitors()
  const dailyById = useLiveDaily()
  const regionsByMonitor = useAllMonitorRegions()
  const regionLabels = useRegionLabels()
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl">Monitors</h1>
        <Link to="/monitors/new">
          <Button>
            <Plus className="h-4 w-4" />
            New monitor
          </Button>
        </Link>
      </div>

      {monitors == null && <Loading />}

      {monitors != null && monitors.length === 0 && (
        <Card>
          <EmptyState
            title="No monitors yet"
            description="Create your first monitor to start tracking uptime."
            action={
              <Link to="/monitors/new">
                <Button>
                  <Plus className="h-4 w-4" />
                  Create monitor
                </Button>
              </Link>
            }
          />
        </Card>
      )}

      {monitors != null && monitors.length > 0 && (
        <div className="glass-card overflow-hidden">
          <Table>
            <TableHead>
              <TableHeader>Name</TableHeader>
              <TableHeader>Type</TableHeader>
              <TableHeader>Status</TableHeader>
              <TableHeader>Uptime (90d)</TableHeader>
              <TableHeader>Target</TableHeader>
              <TableHeader>Last check</TableHeader>
            </TableHead>
            <TableBody>
              {monitors.map((m) => {
                const regions = regionsByMonitor.get(m.id) ?? []
                const upCount = regions.filter((r) => r.status === 'up').length
                const isOpen = expanded.has(m.id)
                return (
                  <Fragment key={m.id}>
                    <TableRow>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {regions.length > 0 && (
                            <button
                              type="button"
                              onClick={() => toggle(m.id)}
                              aria-label={isOpen ? 'Collapse regions' : 'Expand regions'}
                              aria-expanded={isOpen}
                              className="text-text-muted transition-colors hover:text-primary-dark"
                            >
                              <ChevronRight
                                className={`h-4 w-4 transition-transform duration-150 ${
                                  isOpen ? 'rotate-90' : ''
                                }`}
                              />
                            </button>
                          )}
                          <Link
                            to="/monitors/$monitorId"
                            params={{ monitorId: m.id }}
                            className="font-mono font-medium text-primary-dark hover:underline"
                          >
                            {m.name}
                          </Link>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant="outline"
                          className="border border-border-subtle px-1.5 py-0.5 font-mono text-[10px] uppercase"
                        >
                          {m.kind.toUpperCase()}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {m.enabled ? (
                          <div className="flex items-center gap-2">
                            <StatusBadge status={m.status} />
                            {regions.length > 0 && (
                              <span className="font-mono text-xs text-text-muted">
                                {upCount}/{regions.length}
                              </span>
                            )}
                          </div>
                        ) : (
                          <span className="text-sm text-text-muted">Paused</span>
                        )}
                      </TableCell>
                      <TableCell className="min-w-48">
                        <DailyUptimeBar daily={dailyById.get(m.id) ?? []} height="h-4" />
                      </TableCell>
                      <TableCell className="max-w-64 truncate text-text-muted">{m.target}</TableCell>
                      <TableCell className="text-text-muted">
                        <RelativeTime iso={m.last_checked_at} />
                      </TableCell>
                    </TableRow>
                    {isOpen && <RegionRows monitorId={m.id} regions={regions} labels={regionLabels} />}
                  </Fragment>
                )
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}
