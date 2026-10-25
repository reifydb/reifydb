// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useRef, useState, type PointerEvent, type WheelEvent } from 'react'
import { CANVAS_SIZE, TILE_SIZE, type TileKey, inCanvas, parseTileKey, tilesInRect } from '@/geometry'
import { PALETTE, cssColor } from '@/palette'
import type { Pending } from '@/pending'
import type { PixelBuffer, Tile } from '@/pixels'
import { SHAPES, type Point, type ShapeTool, type Tool, isShape, linePixels } from '@/tools'
import { isTyping } from './toolbar'
import { ZoomControl } from './zoom-control'

export const MIN_ZOOM = 1
export const MAX_ZOOM = 32

const ZOOM_STEP = 1.25

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
  tool: Tool
  color: number
  onStroke: (points: Point[]) => void
  onShape: (points: Point[]) => void
  onPick: (point: Point) => void
  onViewport: (viewport: Viewport) => void
  onHover: (pixel: Point | null) => void
}

type Gesture =
  | { kind: 'pan'; startX: number; startY: number; view: View }
  | { kind: 'stroke'; last: Point }
  | { kind: 'shape'; shape: ShapeTool; start: Point }

export function CanvasView({ buffer, pending, tool, color, onStroke, onShape, onPick, onViewport, onHover }: CanvasViewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const bitmapsRef = useRef(new Map<TileKey, HTMLCanvasElement>())
  const gestureRef = useRef<Gesture | null>(null)
  const [size, setSize] = useState({ width: 0, height: 0 })
  const [view, setView] = useState<View>({ zoom: MIN_ZOOM, x: 0, y: 0 })
  const [space, setSpace] = useState(false)
  const [panning, setPanning] = useState(false)
  const [preview, setPreview] = useState<Point[] | null>(null)

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
      if (preview) {
        context.fillStyle = cssColor(color)
        for (const point of preview) {
          if (!inCanvas(point.x, point.y)) continue
          context.fillRect(Math.floor(view.x + point.x * view.zoom), Math.floor(view.y + point.y * view.zoom), cell, cell)
        }
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
  }, [buffer, pending, view, size, preview, color])

  const zoomAt = useCallback(
    (zoom: number, cx: number, cy: number) => {
      setView((current) => {
        const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, zoom))
        const scale = next / current.zoom
        return clampView({ zoom: next, x: cx - (cx - current.x) * scale, y: cy - (cy - current.y) * scale }, size)
      })
    },
    [size],
  )

  const zoomIn = useCallback(() => zoomAt(view.zoom * ZOOM_STEP, size.width / 2, size.height / 2), [zoomAt, view.zoom, size])
  const zoomOut = useCallback(() => zoomAt(view.zoom / ZOOM_STEP, size.width / 2, size.height / 2), [zoomAt, view.zoom, size])
  const reset = useCallback(() => zoomAt(MIN_ZOOM, size.width / 2, size.height / 2), [zoomAt, size])
  const fit = useCallback(() => {
    const span = CANVAS_SIZE * MIN_ZOOM
    setView(clampView({ zoom: MIN_ZOOM, x: (size.width - span) / 2, y: (size.height - span) / 2 }, size))
  }, [size])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.metaKey || event.altKey || isTyping(event.target)) return
      if (event.key === ' ') {
        event.preventDefault()
        setSpace(true)
      } else if (event.key === '+' || event.key === '=') zoomIn()
      else if (event.key === '-') zoomOut()
      else if (event.key === '0') reset()
      else if (event.key === '1') fit()
    }
    const onKeyUp = (event: KeyboardEvent) => {
      if (event.key === ' ') setSpace(false)
    }
    window.addEventListener('keydown', onKeyDown)
    window.addEventListener('keyup', onKeyUp)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
      window.removeEventListener('keyup', onKeyUp)
    }
  }, [zoomIn, zoomOut, reset, fit])

  const pointAt = useCallback(
    (clientX: number, clientY: number): Point | null => {
      const rect = containerRef.current?.getBoundingClientRect()
      if (!rect) return null
      return {
        x: Math.floor((clientX - rect.left - view.x) / view.zoom),
        y: Math.floor((clientY - rect.top - view.y) / view.zoom),
      }
    },
    [view],
  )

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    if (event.button === 1 || ((space || tool === 'pan') && event.button === 0)) {
      event.preventDefault()
      event.currentTarget.setPointerCapture(event.pointerId)
      gestureRef.current = { kind: 'pan', startX: event.clientX, startY: event.clientY, view }
      setPanning(true)
      return
    }
    if (event.button !== 0) return
    const point = pointAt(event.clientX, event.clientY)
    if (!point) return
    event.currentTarget.setPointerCapture(event.pointerId)
    if (tool === 'pencil' || tool === 'eraser') {
      gestureRef.current = { kind: 'stroke', last: point }
      onStroke([point])
    } else if (isShape(tool)) {
      gestureRef.current = { kind: 'shape', shape: tool, start: point }
      setPreview(SHAPES[tool](point, point))
    } else if (tool === 'eyedropper' && inCanvas(point.x, point.y)) {
      onPick(point)
    }
  }

  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    const point = pointAt(event.clientX, event.clientY)
    onHover(point && inCanvas(point.x, point.y) ? point : null)
    const gesture = gestureRef.current
    if (!gesture || !point) return
    if (gesture.kind === 'pan') {
      const dx = event.clientX - gesture.startX
      const dy = event.clientY - gesture.startY
      setView(clampView({ zoom: gesture.view.zoom, x: gesture.view.x + dx, y: gesture.view.y + dy }, size))
    } else if (gesture.kind === 'stroke') {
      if (point.x === gesture.last.x && point.y === gesture.last.y) return
      onStroke(linePixels(gesture.last, point).slice(1))
      gesture.last = point
    } else {
      setPreview(SHAPES[gesture.shape](gesture.start, point))
    }
  }

  const onPointerUp = (event: PointerEvent<HTMLDivElement>) => {
    const gesture = gestureRef.current
    gestureRef.current = null
    setPanning(false)
    if (gesture?.kind !== 'shape') return
    setPreview(null)
    const point = pointAt(event.clientX, event.clientY) ?? gesture.start
    onShape(SHAPES[gesture.shape](gesture.start, point))
  }

  const onPointerCancel = () => {
    gestureRef.current = null
    setPanning(false)
    setPreview(null)
  }

  const onWheel = (event: WheelEvent<HTMLDivElement>) => {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    zoomAt(view.zoom * (event.deltaY < 0 ? ZOOM_STEP : 1 / ZOOM_STEP), event.clientX - rect.left, event.clientY - rect.top)
  }

  const cursor = panning ? 'cursor-grabbing' : space || tool === 'pan' ? 'cursor-grab' : 'cursor-crosshair'

  return (
    <div
      ref={containerRef}
      className={`relative h-full min-h-[480px] w-full ${cursor} touch-none select-none overflow-hidden border-2 border-border-default bg-bg-tertiary`}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerCancel={onPointerCancel}
      onPointerLeave={() => onHover(null)}
      onWheel={onWheel}
    >
      <canvas ref={canvasRef} className="absolute inset-0 [image-rendering:pixelated]" />
      <ZoomControl zoom={view.zoom} onZoomIn={zoomIn} onZoomOut={zoomOut} onReset={reset} onFit={fit} />
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
