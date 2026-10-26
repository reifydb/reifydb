// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Eye, EyeOff } from 'lucide-react'
import { MAX_PENDING } from '@/geometry'
import { PalettePicker } from './palette-picker'

interface BottomBarProps {
  color: number
  onColor: (color: number) => void
  hover: { x: number; y: number } | null
  count: number
  layerVisible: boolean
  onLayerVisible: (visible: boolean) => void
  committing: boolean
  onCommit: () => void
  onClear: () => void
  notice: string | null
  message: string | null
}

const NEAR_FULL = Math.ceil(MAX_PENDING * 0.9)

export function BottomBar({
  color,
  onColor,
  hover,
  count,
  layerVisible,
  onLayerVisible,
  committing,
  onCommit,
  onClear,
  notice,
  message,
}: BottomBarProps) {
  const full = count >= MAX_PENDING
  const nearFull = count >= NEAR_FULL
  const hint = notice ?? (full ? 'Layer full. Merge it to keep drawing.' : nearFull ? 'Layer almost full. Merge soon.' : null)
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
          <span className="flex items-center gap-1 font-mono text-xs uppercase tracking-[1.4px] text-text-muted">
            <button
              type="button"
              aria-label={layerVisible ? 'Hide your layer' : 'Show your layer'}
              aria-pressed={!layerVisible}
              title={layerVisible ? 'Hide your layer' : 'Show your layer'}
              onClick={() => onLayerVisible(!layerVisible)}
              className="text-[#2a2f8f] outline-none hover:text-primary-dark"
            >
              {layerVisible ? <Eye size={14} /> : <EyeOff size={14} />}
            </button>
            Your layer
          </span>
          <span className={`font-mono text-sm ${level}`}>
            {count} / {MAX_PENDING} px
          </span>
          <span className={`h-4 font-mono text-xs ${level}`} role="status">
            {hint}
          </span>
        </div>
        <button
          type="button"
          className={`game-button ${full ? 'animate-pulse' : ''}`}
          title="Everyone sees it after merging"
          onClick={onCommit}
          disabled={count === 0 || committing}
        >
          {committing ? 'Merging...' : `Merge ${count} px to canvas`}
        </button>
        <button
          type="button"
          className="game-button game-button--secondary"
          title="Removes your unmerged pixels"
          onClick={onClear}
          disabled={count === 0 || committing}
        >
          Delete layer
        </button>
      </div>
      {message && <p className="min-w-0 max-w-xs font-mono text-xs text-status-error">{message}</p>}
    </div>
  )
}
