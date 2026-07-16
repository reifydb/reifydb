// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { MonitorStatus } from '@/lib/types'

const COLORS: Record<MonitorStatus, string> = {
  up: 'bg-status-success',
  down: 'bg-status-error animate-pulse',
  unknown: 'bg-text-muted',
}

export function StatusDot({
  status,
  className = '',
}: {
  status: MonitorStatus
  className?: string
}) {
  return (
    <span
      title={status}
      className={`inline-block h-2.5 w-2.5 rounded-full ${COLORS[status]} ${className}`}
    />
  )
}
