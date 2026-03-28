// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { cn } from '@/lib'

const statusConfig: Record<string, { color: string; label: string }> = {
  pending:   { color: 'bg-status-warning/20 text-status-warning border-status-warning/30', label: 'pending' },
  running:   { color: 'bg-status-info/20 text-status-info border-status-info/30', label: 'running' },
  succeeded: { color: 'bg-status-success/20 text-status-success border-status-success/30', label: 'succeeded' },
  failed:    { color: 'bg-status-error/20 text-status-error border-status-error/30', label: 'failed' },
  cancelled: { color: 'bg-gray-200 text-gray-600 border-gray-400/30', label: 'cancelled' },
  skipped:   { color: 'bg-gray-100 text-gray-500 border-gray-400/20', label: 'skipped' },
}

interface StatusBadgeProps {
  status: string
  className?: string
}

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const config = statusConfig[status] ?? statusConfig['pending']
  return (
    <span className={cn(
      'inline-flex items-center px-2 py-0.5 text-xs font-mono font-medium border',
      config.color,
      className,
    )}>
      {config.label}
    </span>
  )
}
