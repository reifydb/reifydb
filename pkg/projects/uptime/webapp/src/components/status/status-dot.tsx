// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { cn } from '@/lib/utils'
import type { MonitorStatus } from '@/lib/types'

const COLORS: Record<MonitorStatus, string> = {
  up: 'bg-emerald-500',
  down: 'bg-red-500',
  unknown: 'bg-gray-400',
}

export function StatusDot({
  status,
  className,
}: {
  status: MonitorStatus
  className?: string
}) {
  return (
    <span
      title={status}
      className={cn(
        'inline-block h-2.5 w-2.5 rounded-full',
        COLORS[status],
        status === 'down' && 'animate-pulse',
        className,
      )}
    />
  )
}
