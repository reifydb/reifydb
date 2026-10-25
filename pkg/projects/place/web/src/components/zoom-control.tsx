// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Maximize, Minus, Plus } from 'lucide-react'

interface ZoomControlProps {
  zoom: number
  onZoomIn: () => void
  onZoomOut: () => void
  onReset: () => void
  onFit: () => void
}

export function ZoomControl({ zoom, onZoomIn, onZoomOut, onReset, onFit }: ZoomControlProps) {
  const button = 'flex h-9 items-center justify-center text-[#2a2f8f] outline-none hover:bg-[#eef0ff]'
  return (
    <div
      className="panel absolute bottom-4 right-4 z-10 flex gap-1 p-1"
      onPointerDown={(event) => event.stopPropagation()}
      onWheel={(event) => event.stopPropagation()}
    >
      <button type="button" aria-label="Zoom out" title="Zoom out (-)" onClick={onZoomOut} className={`${button} w-9`}>
        <Minus size={16} />
      </button>
      <button type="button" aria-label="Reset zoom" title="Reset to 100% (0)" onClick={onReset} className={`${button} w-16 font-mono text-xs`}>
        {Math.round(zoom * 100)}%
      </button>
      <button type="button" aria-label="Zoom in" title="Zoom in (+)" onClick={onZoomIn} className={`${button} w-9`}>
        <Plus size={16} />
      </button>
      <button type="button" aria-label="Fit" title="Fit (1)" onClick={onFit} className={`${button} w-9`}>
        <Maximize size={16} />
      </button>
    </div>
  )
}
