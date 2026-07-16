// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function UptimePercent({
  ratio,
  className = '',
}: {
  ratio: number | null
  className?: string
}) {
  if (ratio == null) {
    return <span className={`text-text-muted ${className}`}>-</span>
  }
  const pct = ratio * 100
  const formatted = pct >= 99.995 ? '100%' : `${pct.toFixed(2)}%`
  const tone =
    pct >= 99 ? 'text-status-success' : pct >= 90 ? 'text-status-warning' : 'text-status-error'
  return <span className={`${tone} ${className}`}>{formatted}</span>
}
