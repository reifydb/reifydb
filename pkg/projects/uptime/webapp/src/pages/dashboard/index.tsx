// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link } from '@tanstack/react-router'
import { Plus } from 'lucide-react'
import { useMonitors } from '@/hooks/use-monitors'
import { formatRelativeTime } from '@/lib/format'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { StatusBadge } from '@/components/status/status-badge'

export function DashboardPage() {
  const { data: monitors, isLoading, error } = useMonitors()

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Monitors</h1>
        <Link to="/monitors/new">
          <Button>
            <Plus className="h-4 w-4" />
            New monitor
          </Button>
        </Link>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Loading...</p>}
      {error != null && (
        <p className="text-sm text-destructive">Failed to load monitors: {error.message}</p>
      )}

      {monitors != null && monitors.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center space-y-4">
            <p className="text-muted-foreground">
              No monitors yet. Create your first monitor to start tracking uptime.
            </p>
            <Link to="/monitors/new">
              <Button>
                <Plus className="h-4 w-4" />
                Create monitor
              </Button>
            </Link>
          </CardContent>
        </Card>
      )}

      {monitors != null && monitors.length > 0 && (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Target</TableHead>
                <TableHead>Last check</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {monitors.map((m) => (
                <TableRow key={m.id}>
                  <TableCell>
                    <Link
                      to="/monitors/$monitorId"
                      params={{ monitorId: m.id }}
                      className="font-medium text-primary hover:underline"
                    >
                      {m.name}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{m.kind.toUpperCase()}</Badge>
                  </TableCell>
                  <TableCell>
                    {m.enabled ? (
                      <StatusBadge status={m.status} />
                    ) : (
                      <span className="text-sm text-muted-foreground">Paused</span>
                    )}
                  </TableCell>
                  <TableCell className="max-w-64 truncate text-muted-foreground">
                    {m.target}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatRelativeTime(m.last_checked_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  )
}
