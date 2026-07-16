// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { cn } from '@/lib/utils'
import type { CheckResult } from '@/lib/types'

export function UptimeBar({
  results,
  max = 50,
  className,
}: {
  results: CheckResult[]
  max?: number
  className?: string
}) {
  const shown = results.slice(0, max).reverse()
  if (shown.length === 0) {
    return <p className={cn('text-sm text-muted-foreground', className)}>No checks yet</p>
  }
  return (
    <div className={cn('flex items-end gap-0.5', className)}>
      {shown.map((r, i) => (
        <span
          key={`${r.checked_at}-${i}`}
          title={`${new Date(r.checked_at).toLocaleString()} - ${r.success ? 'up' : 'down'}${
            r.response_time_ms != null ? ` (${r.response_time_ms} ms)` : ''
          }${r.error != null ? ` - ${r.error}` : ''}`}
          className={cn(
            'h-6 w-1.5 rounded-sm',
            r.success ? 'bg-emerald-500' : 'bg-red-500',
          )}
        />
      ))}
    </div>
  )
}
