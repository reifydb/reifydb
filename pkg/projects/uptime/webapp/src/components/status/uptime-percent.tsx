// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { cn } from '@/lib/utils'

export function UptimePercent({
  ratio,
  className,
}: {
  ratio: number | null
  className?: string
}) {
  if (ratio == null) {
    return <span className={cn('text-muted-foreground', className)}>-</span>
  }
  const pct = ratio * 100
  const formatted = pct >= 99.995 ? '100%' : `${pct.toFixed(2)}%`
  return (
    <span
      className={cn(
        pct >= 99 ? 'text-emerald-600' : pct >= 90 ? 'text-amber-600' : 'text-red-600',
        className,
      )}
    >
      {formatted}
    </span>
  )
}
