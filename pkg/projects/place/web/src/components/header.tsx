// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Badge } from '@reifydb/ui'

const STATUS = {
  connecting: { label: 'Connecting', variant: 'warning' },
  ready: { label: 'Live', variant: 'success' },
  failed: { label: 'Offline', variant: 'danger' },
} as const

export function Header({ status }: { status: keyof typeof STATUS }) {
  const { label, variant } = STATUS[status]
  return (
    <header className="sticky top-0 z-40 w-full border-b-2 border-border-default bg-bg-primary">
      <div className="mx-auto flex h-[60px] max-w-[1600px] items-center justify-between px-4 sm:px-6">
        <div className="flex items-baseline gap-3">
          <span className="font-mono text-lg font-bold tracking-tight text-text-primary">Place</span>
          <span className="hidden font-mono text-xs uppercase tracking-[1.4px] text-text-secondary sm:inline">
            2048 x 2048 shared canvas
          </span>
        </div>
        <Badge variant={variant}>{label}</Badge>
      </div>
    </header>
  )
}
