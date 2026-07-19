// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useParams } from '@tanstack/react-router'
import { usePublicStatus } from '@/hooks/use-public-status'
import { ApiError } from '@/lib/api'
import type { DailyUptime } from '@/lib/types'
import { RelativeTime } from '@/components/relative-time'
import { PublicLayout } from '@/components/layout/public-layout'
import { Card, EmptyState, Loading } from '@reifydb/ui'
import { DailyUptimeBar } from '@/components/status/daily-uptime-bar'
import { StatusBadge } from '@/components/status/status-badge'
import { UptimePercent } from '@/components/status/uptime-percent'

function overallRatio(daily: DailyUptime[]): number | null {
  const total = daily.reduce((a, d) => a + d.total, 0)
  if (total === 0) return null
  return daily.reduce((a, d) => a + d.up, 0) / total
}

function OverallBanner({ statuses }: { statuses: string[] }) {
  const down = statuses.filter((s) => s === 'down').length
  const all_up = statuses.length > 0 && statuses.every((s) => s === 'up')
  const label = all_up
    ? 'All systems operational'
    : down > 0
      ? `${down} of ${statuses.length} systems down`
      : 'Status partially unknown'
  const tone = all_up
    ? 'bg-status-success/10'
    : down > 0
      ? 'bg-status-error/10'
      : 'bg-status-warning/10'
  return (
    <div
      className={`border-2 border-border-default px-4 py-3 font-mono text-sm font-bold text-text-primary shadow-[var(--shadow-hard)] ${tone}`}
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
      {isLoading && <Loading />}

      {error instanceof ApiError && error.status === 404 && (
        <Card>
          <EmptyState title="This status page does not exist." />
        </Card>
      )}
      {error != null && !(error instanceof ApiError && error.status === 404) && (
        <p className="text-sm text-status-error">Failed to load status page.</p>
      )}

      {data != null && (
        <div className="space-y-6">
          <h1 className="text-2xl">{data.title}</h1>
          <OverallBanner statuses={data.monitors.map((m) => m.status)} />
          <div className="glass-card divide-y divide-border-light">
            {data.monitors.map((m) => (
              <div key={m.name} className="space-y-3 px-6 py-5">
                <div className="flex items-center justify-between gap-4">
                  <div className="min-w-0">
                    <p className="truncate font-mono font-medium">{m.name}</p>
                    <p className="text-xs text-text-muted">
                      Last check <RelativeTime iso={m.last_checked_at} />
                    </p>
                  </div>
                  <div className="flex items-center gap-4">
                    <UptimePercent ratio={overallRatio(m.daily)} className="text-sm font-medium" />
                    <StatusBadge status={m.status} />
                  </div>
                </div>
                <DailyUptimeBar daily={m.daily} />
                <div className="flex justify-between font-mono text-xs text-text-muted">
                  <span>90 days ago</span>
                  <span>today</span>
                </div>
              </div>
            ))}
            {data.monitors.length === 0 && (
              <p className="px-6 py-8 text-center text-sm text-text-muted">
                No monitors on this status page yet.
              </p>
            )}
          </div>
        </div>
      )}
    </PublicLayout>
  )
}
