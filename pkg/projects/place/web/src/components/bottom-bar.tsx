// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
  notice: string | null
  message: string | null
}

const NEAR_FULL = Math.ceil(MAX_PENDING * 0.9)

export function BottomBar({ color, onColor, hover, count, committing, onCommit, onClear, notice, message }: BottomBarProps) {
  const full = count >= MAX_PENDING
  const nearFull = count >= NEAR_FULL
  const hint = notice ?? (full ? 'Full. Commit to keep drawing.' : nearFull ? 'Almost full. Commit soon.' : null)
  const level = full || notice ? 'text-status-error' : nearFull ? 'text-status-warning' : 'text-text-primary'
  return (
    <div className="panel absolute bottom-4 left-1/2 z-10 flex w-[calc(100%-2rem)] max-w-max -translate-x-1/2 flex-col gap-3 p-3 lg:flex-row lg:items-center">
      <div className="lg:w-[384px] lg:shrink-0">
        <PalettePicker selected={color} onSelect={onColor} />
      </div>
      <span className="w-28 shrink-0 font-mono text-xs uppercase tracking-[1.4px] text-text-muted">
        {hover ? `x ${hover.x}  y ${hover.y}` : 'x -  y -'}
      </span>
      <div className="flex shrink-0 items-center gap-2">
        <div className="flex w-52 flex-col">
          <span className={`font-mono text-sm ${level}`}>
            {count} / {MAX_PENDING}
          </span>
          <span className={`h-4 font-mono text-xs ${level}`} role="status">
            {hint}
          </span>
        </div>
        <button type="button" className={`game-button ${full ? 'animate-pulse' : ''}`} onClick={onCommit} disabled={count === 0 || committing}>
          {committing ? 'Committing' : `Commit ${count}`}
        </button>
        <button type="button" className="game-button game-button--secondary" onClick={onClear} disabled={count === 0 || committing}>
          Clear
        </button>
      </div>
      {message && <p className="min-w-0 max-w-xs font-mono text-xs text-status-error">{message}</p>}
    </div>
  )
}
