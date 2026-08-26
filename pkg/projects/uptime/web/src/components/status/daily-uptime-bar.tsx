// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { DailyUptime } from '@/lib/types'

function dayAxis(days: number): string[] {
  const out: string[] = []
  const now = Date.now()
  for (let i = days - 1; i >= 0; i--) {
    out.push(new Date(now - i * 86_400_000).toISOString().slice(0, 10))
  }
  return out
}

export function DailyUptimeBar({
  daily,
  days = 90,
  height = 'h-8',
  className = '',
}: {
  daily: DailyUptime[]
  days?: number
  height?: string
  className?: string
}) {
  const byDay = new Map(daily.map((d) => [d.day, d]))
  return (
    <div className={`flex ${height} gap-px ${className}`}>
      {dayAxis(days).map((day) => {
        const bucket = byDay.get(day)
        if (bucket == null || bucket.total === 0) {
          return (
            <span
              key={day}
              title={`${day} - no checks`}
              className="h-full flex-1 rounded-none bg-bg-elevated"
            />
          )
        }
        const ratio = bucket.up / bucket.total
        const tone =
          ratio >= 0.99
            ? 'bg-status-success'
            : ratio >= 0.9
              ? 'bg-status-warning'
              : 'bg-status-error'
        return (
          <span
            key={day}
            title={`${day} - up ${bucket.up}/${bucket.total} (${(ratio * 100).toFixed(1)}%)`}
            className={`h-full flex-1 rounded-none ${tone}`}
          />
        )
      })}
    </div>
  )
}
