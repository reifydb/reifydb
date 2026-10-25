// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Button } from '@reifydb/ui'
import { MAX_PENDING } from '@/geometry'
import { PalettePicker } from './palette-picker'

interface BottomBarProps {
  color: number
  onColor: (color: number) => void
  hover: { x: number; y: number } | null
  count: number
  committing: boolean
  onCommit: () => void
  onClear: () => void
  message: string | null
}

export function BottomBar({ color, onColor, hover, count, committing, onCommit, onClear, message }: BottomBarProps) {
  return (
    <div className="flex flex-col gap-3 border-2 border-t-0 border-border-default bg-bg-primary p-3 lg:flex-row lg:items-center">
      <div className="lg:w-[512px] lg:shrink-0">
        <PalettePicker selected={color} onSelect={onColor} />
      </div>
      <span className="w-28 shrink-0 font-mono text-xs uppercase tracking-[1.4px] text-text-muted">
        {hover ? `x ${hover.x}  y ${hover.y}` : 'x -  y -'}
      </span>
      <div className="flex shrink-0 items-center gap-2">
        <span className="font-mono text-sm text-text-primary">
          {count} / {MAX_PENDING}
        </span>
        <Button onClick={onCommit} disabled={count === 0 || committing}>
          {committing ? 'Committing' : `Commit ${count}`}
        </Button>
        <Button variant="secondary" onClick={onClear} disabled={count === 0 || committing}>
          Clear
        </Button>
      </div>
      {message && <p className="min-w-0 font-mono text-xs text-status-error">{message}</p>}
    </div>
  )
}
