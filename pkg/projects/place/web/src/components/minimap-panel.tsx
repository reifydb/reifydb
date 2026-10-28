// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef, type PointerEvent, type ReactNode } from 'react'
import { MINIMAP_CSS_SIZE, MINIMAP_SIZE, type Minimap, minimapToCanvas, viewBox } from '@/minimap'
import type { Point } from '@/tools'

interface MinimapPanelProps {
  minimap: Minimap
  view: { zoom: number; x: number; y: number }
  size: { width: number; height: number }
  onCenter: (point: Point) => void
  footer: ReactNode
}

export function MinimapPanel({ minimap, view, size, onCenter, footer }: MinimapPanelProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const context = canvasRef.current?.getContext('2d')
    if (!context) return
    const image = new ImageData(minimap.pixels, MINIMAP_SIZE, MINIMAP_SIZE)
    minimap.takeDirty()
    context.putImageData(image, 0, 0)
    let frame = 0
    const flush = () => {
      frame = 0
      const dirty = minimap.takeDirty()
      if (dirty) context.putImageData(image, 0, 0, dirty.x, dirty.y, dirty.width, dirty.height)
    }
    const unsubscribe = minimap.subscribe(() => {
      if (frame === 0) frame = requestAnimationFrame(flush)
    })
    return () => {
      unsubscribe()
      if (frame !== 0) cancelAnimationFrame(frame)
    }
  }, [minimap])

  const centerAt = (event: PointerEvent<HTMLDivElement>) => {
    const rect = event.currentTarget.getBoundingClientRect()
    const clamp = (v: number) => Math.min(MINIMAP_CSS_SIZE, Math.max(0, v))
    onCenter(minimapToCanvas(clamp(event.clientX - rect.left), clamp(event.clientY - rect.top)))
  }

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return
    event.currentTarget.setPointerCapture(event.pointerId)
    centerAt(event)
  }

  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) centerAt(event)
  }

  const box = viewBox(view, size)

  return (
    <div
      className="panel absolute right-4 top-4 z-10 p-1"
      onPointerDown={(event) => event.stopPropagation()}
      onPointerMove={(event) => event.stopPropagation()}
      onPointerUp={(event) => event.stopPropagation()}
      onWheel={(event) => event.stopPropagation()}
    >
      <div
        className="dot-grid relative cursor-pointer overflow-hidden"
        style={{ width: MINIMAP_CSS_SIZE, height: MINIMAP_CSS_SIZE }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
      >
        <canvas ref={canvasRef} width={MINIMAP_SIZE} height={MINIMAP_SIZE} className="absolute inset-0 h-full w-full" />
        <div
          className="pointer-events-none absolute border border-[#4b52d6]"
          style={{ left: box.x, top: box.y, width: box.width, height: box.height }}
        />
      </div>
      {footer}
    </div>
  )
}
