// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useRef, useState, type PointerEvent, type WheelEvent } from 'react'
import { CANVAS_SIZE, TILE_SIZE, type TileKey, parseTileKey, tilesInRect } from '@/geometry'
import { PALETTE, cssColor } from '@/palette'
import type { Pending } from '@/pending'
import type { PixelBuffer, Tile } from '@/pixels'

export const MIN_ZOOM = 1
export const MAX_ZOOM = 32

export interface Viewport {
  left: number
  top: number
  right: number
  bottom: number
}

interface View {
  zoom: number
  x: number
  y: number
}

interface CanvasViewProps {
  buffer: PixelBuffer
  pending: Pending
  onPaint: (x: number, y: number) => void
  onViewport: (viewport: Viewport) => void
  onHover: (pixel: { x: number; y: number } | null) => void
}

const DRAG_THRESHOLD = 4

export function CanvasView({ buffer, pending, onPaint, onViewport, onHover }: CanvasViewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const bitmapsRef = useRef(new Map<TileKey, HTMLCanvasElement>())
  const dragRef = useRef<{ startX: number; startY: number; view: View; moved: boolean } | null>(null)
  const [size, setSize] = useState({ width: 0, height: 0 })
  const [view, setView] = useState<View>({ zoom: MIN_ZOOM, x: 0, y: 0 })

  useEffect(() => {
    const container = containerRef.current
    if (!container) return
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect
      setSize({ width, height })
    })
    observer.observe(container)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    setView((current) => clampView(current, size))
  }, [size])

  useEffect(() => {
    if (size.width === 0 || size.height === 0) return
    onViewport({
      left: -view.x / view.zoom,
      top: -view.y / view.zoom,
      right: (size.width - view.x) / view.zoom,
      bottom: (size.height - view.y) / view.zoom,
    })
  }, [view, size, onViewport])

  useEffect(() => {
    const canvas = canvasRef.current
    const context = canvas?.getContext('2d')
    if (!canvas || !context) return
    let frame = 0
    const flush = () => {
      frame = 0
      const bitmaps = bitmapsRef.current
      for (const key of buffer.takeDirty()) {
        const tile = buffer.tile(key)
        if (tile) bitmaps.set(key, tileBitmap(tile))
        else bitmaps.delete(key)
      }
      if (canvas.width !== size.width || canvas.height !== size.height) {
        canvas.width = size.width
        canvas.height = size.height
      }
      context.clearRect(0, 0, canvas.width, canvas.height)
      context.imageSmoothingEnabled = false
      const span = TILE_SIZE * view.zoom
      const visible = tilesInRect(
        -view.x / view.zoom,
        -view.y / view.zoom,
        (size.width - view.x) / view.zoom,
        (size.height - view.y) / view.zoom,
      )
      for (const key of visible) {
        const bitmap = bitmaps.get(key)
        if (!bitmap) continue
        const [tx, ty] = parseTileKey(key)
        context.drawImage(bitmap, view.x + tx * span, view.y + ty * span, span, span)
      }
      const cell = Math.ceil(view.zoom)
      for (const pixel of pending.entries()) {
        context.fillStyle = cssColor(pixel.color)
        context.fillRect(Math.floor(view.x + pixel.x * view.zoom), Math.floor(view.y + pixel.y * view.zoom), cell, cell)
      }
    }
    const schedule = () => {
      if (frame === 0) frame = requestAnimationFrame(flush)
    }
    schedule()
    const unsubscribeBuffer = buffer.subscribe(schedule)
    const unsubscribePending = pending.subscribe(schedule)
    return () => {
      unsubscribeBuffer()
      unsubscribePending()
      if (frame !== 0) cancelAnimationFrame(frame)
    }
  }, [buffer, pending, view, size])

  const pixelAt = useCallback(
    (clientX: number, clientY: number) => {
      const rect = containerRef.current?.getBoundingClientRect()
      if (!rect) return null
      const x = Math.floor((clientX - rect.left - view.x) / view.zoom)
      const y = Math.floor((clientY - rect.top - view.y) / view.zoom)
      if (x < 0 || y < 0 || x >= CANVAS_SIZE || y >= CANVAS_SIZE) return null
      return { x, y }
    },
    [view],
  )

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    event.currentTarget.setPointerCapture(event.pointerId)
    dragRef.current = { startX: event.clientX, startY: event.clientY, view, moved: false }
  }

  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    onHover(pixelAt(event.clientX, event.clientY))
    const drag = dragRef.current
    if (!drag) return
    const dx = event.clientX - drag.startX
    const dy = event.clientY - drag.startY
    if (!drag.moved && Math.hypot(dx, dy) < DRAG_THRESHOLD) return
    drag.moved = true
    setView(clampView({ zoom: drag.view.zoom, x: drag.view.x + dx, y: drag.view.y + dy }, size))
  }

  const onPointerUp = (event: PointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current
    dragRef.current = null
    if (!drag || drag.moved) return
    const pixel = pixelAt(event.clientX, event.clientY)
    if (pixel) onPaint(pixel.x, pixel.y)
  }

  const onWheel = (event: WheelEvent<HTMLDivElement>) => {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    const zoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, view.zoom * (event.deltaY < 0 ? 1.25 : 0.8)))
    const cx = event.clientX - rect.left
    const cy = event.clientY - rect.top
    const scale = zoom / view.zoom
    setView(clampView({ zoom, x: cx - (cx - view.x) * scale, y: cy - (cy - view.y) * scale }, size))
  }

  return (
    <div
      ref={containerRef}
      className="relative h-full min-h-[480px] w-full cursor-crosshair touch-none select-none overflow-hidden border-2 border-border-default bg-bg-tertiary"
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerLeave={() => onHover(null)}
      onWheel={onWheel}
    >
      <canvas ref={canvasRef} className="absolute inset-0 [image-rendering:pixelated]" />
    </div>
  )
}

function tileBitmap(tile: Tile): HTMLCanvasElement {
  const bitmap = document.createElement('canvas')
  bitmap.width = TILE_SIZE
  bitmap.height = TILE_SIZE
  const context = bitmap.getContext('2d')
  if (!context) throw new Error('2d canvas context unavailable')
  const image = context.createImageData(TILE_SIZE, TILE_SIZE)
  for (let i = 0; i < tile.bytes.length; i++) {
    const rgb = PALETTE[tile.bytes[i]]
    image.data[i * 4] = (rgb >> 16) & 0xff
    image.data[i * 4 + 1] = (rgb >> 8) & 0xff
    image.data[i * 4 + 2] = rgb & 0xff
    image.data[i * 4 + 3] = 0xff
  }
  context.putImageData(image, 0, 0)
  return bitmap
}

function clampView(view: View, size: { width: number; height: number }): View {
  const span = CANVAS_SIZE * view.zoom
  const clampAxis = (offset: number, extent: number) =>
    span <= extent ? (extent - span) / 2 : Math.min(0, Math.max(extent - span, offset))
  return { zoom: view.zoom, x: clampAxis(view.x, size.width), y: clampAxis(view.y, size.height) }
}
