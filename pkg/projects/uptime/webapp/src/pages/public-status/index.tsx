// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useParams } from '@tanstack/react-router'
import { usePublicStatus } from '@/hooks/use-public-status'
import { ApiError } from '@/lib/api'
import { formatRelativeTime } from '@/lib/format'
import { cn } from '@/lib/utils'
import { PublicLayout } from '@/components/layout/public-layout'
import { Card, CardContent } from '@/components/ui/card'
import { StatusBadge } from '@/components/status/status-badge'
import { UptimePercent } from '@/components/status/uptime-percent'

function OverallBanner({ statuses }: { statuses: string[] }) {
  const down = statuses.filter((s) => s === 'down').length
  const all_up = statuses.length > 0 && statuses.every((s) => s === 'up')
  const label = all_up
    ? 'All systems operational'
    : down > 0
      ? `${down} of ${statuses.length} systems down`
      : 'Status partially unknown'
  return (
    <div
      className={cn(
        'px-4 py-3 text-sm font-medium border',
        all_up
          ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-700'
          : down > 0
            ? 'bg-red-500/10 border-red-500/40 text-red-700'
            : 'bg-amber-500/10 border-amber-500/40 text-amber-700',
      )}
    >
      {label}
    </div>
  )
}

export function PublicStatusPage() {
  const { slug } = useParams({ strict: false }) as { slug: string }
  const { data, isLoading, error } = usePublicStatus(slug)

  return (
    <PublicLayout>
      {isLoading && <p className="text-sm text-muted-foreground">Loading...</p>}

      {error instanceof ApiError && error.status === 404 && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            This status page does not exist.
          </CardContent>
        </Card>
      )}
      {error != null && !(error instanceof ApiError && error.status === 404) && (
        <p className="text-sm text-destructive">Failed to load status page.</p>
      )}

      {data != null && (
        <div className="space-y-6">
          <h1 className="text-2xl font-semibold">{data.title}</h1>
          <OverallBanner statuses={data.monitors.map((m) => m.status)} />
          <Card>
            <CardContent className="divide-y divide-border p-0">
              {data.monitors.map((m) => (
                <div key={m.name} className="flex items-center justify-between px-4 py-3">
                  <div className="space-y-1">
                    <p className="font-medium">{m.name}</p>
                    <p className="text-xs text-muted-foreground">
                      Last check {formatRelativeTime(m.last_checked_at)}
                    </p>
                  </div>
                  <div className="flex items-center gap-6">
                    <div className="text-right">
                      <UptimePercent ratio={m.uptime_24h} className="text-sm font-medium" />
                      <p className="text-xs text-muted-foreground">24h uptime</p>
                    </div>
                    <StatusBadge status={m.status} />
                  </div>
                </div>
              ))}
              {data.monitors.length === 0 && (
                <p className="px-4 py-8 text-center text-sm text-muted-foreground">
                  No monitors on this status page yet.
                </p>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </PublicLayout>
  )
}
