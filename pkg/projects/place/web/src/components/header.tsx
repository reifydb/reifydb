// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Badge } from '@reifydb/ui'
import { CANVAS_SIZE } from '@/geometry'

const STATUS = {
  connecting: { label: 'Connecting', variant: 'warning' },
  ready: { label: 'Live', variant: 'success' },
  failed: { label: 'Offline', variant: 'danger' },
} as const

export function Header({ status }: { status: keyof typeof STATUS }) {
  const { label, variant } = STATUS[status]
  return (
    <header className="z-40 w-full shrink-0 border-b-2 border-[#4b52d6] bg-bg-primary">
      <div className="flex h-[60px] w-full items-center justify-between px-4 sm:px-6 md:px-8">
        <div className="flex items-baseline gap-3">
          <span className="font-mono text-lg font-bold tracking-tight text-text-primary">Place</span>
          <span className="hidden font-mono text-xs uppercase tracking-[1.4px] text-text-secondary sm:inline">
            {CANVAS_SIZE} x {CANVAS_SIZE} shared canvas
          </span>
        </div>
        <Badge variant={variant}>{label}</Badge>
      </div>
    </header>
  )
}
