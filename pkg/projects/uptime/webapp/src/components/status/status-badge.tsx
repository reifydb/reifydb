// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { cn } from '@/lib/utils'
import type { MonitorStatus } from '@/lib/types'
import { StatusDot } from './status-dot'

const LABELS: Record<MonitorStatus, string> = {
  up: 'Up',
  down: 'Down',
  unknown: 'Pending',
}

export function StatusBadge({
  status,
  className,
}: {
  status: MonitorStatus
  className?: string
}) {
  return (
    <span className={cn('inline-flex items-center gap-2 text-sm', className)}>
      <StatusDot status={status} />
      {LABELS[status]}
    </span>
  )
}
