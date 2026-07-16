// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link } from '@tanstack/react-router'
import { Plus } from 'lucide-react'
import { useMonitors, useMonitorsDaily } from '@/hooks/use-monitors'
import { formatRelativeTime } from '@/lib/format'
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

export function DashboardPage() {
  const { data: monitors, isLoading, error } = useMonitors()
  const { data: dailyData } = useMonitorsDaily()
  const dailyById = new Map((dailyData ?? []).map((d) => [d.monitor_id, d.daily]))

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

      {isLoading && <Loading />}
      {error != null && (
        <p className="text-sm text-status-error">Failed to load monitors: {error.message}</p>
      )}

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
              {monitors.map((m) => (
                <TableRow key={m.id}>
                  <TableCell>
                    <Link
                      to="/monitors/$monitorId"
                      params={{ monitorId: m.id }}
                      className="font-mono font-medium text-primary-dark hover:underline"
                    >
                      {m.name}
                    </Link>
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
                      <StatusBadge status={m.status} />
                    ) : (
                      <span className="text-sm text-text-muted">Paused</span>
                    )}
                  </TableCell>
                  <TableCell className="min-w-48">
                    {dailyById.has(m.id) ? (
                      <DailyUptimeBar daily={dailyById.get(m.id) ?? []} height="h-4" />
                    ) : (
                      <span className="text-sm text-text-muted">-</span>
                    )}
                  </TableCell>
                  <TableCell className="max-w-64 truncate text-text-muted">
                    {m.target}
                  </TableCell>
                  <TableCell className="text-text-muted">
                    {formatRelativeTime(m.last_checked_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}
