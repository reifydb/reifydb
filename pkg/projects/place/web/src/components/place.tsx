// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import type { PlaceClient } from '@/client'
import { commitPending } from '@/commit'
import { MAX_PENDING, inCanvas, tilesInRect } from '@/geometry'
import { type Stamp, convert, fitSize, preShrinkSize, stampPixels } from '@/image'
import { Pending } from '@/pending'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'
import type { Point, Tool } from '@/tools'
import { BottomBar } from './bottom-bar'
import { CanvasView, type Viewport } from './canvas-view'
import { Toolbar } from './toolbar'

const VIEWPORT_DEBOUNCE_MS = 150

export function Place({ client }: { client: PlaceClient }) {
  const buffer = useMemo(() => new PixelBuffer(), [])
  const pending = useMemo(() => new Pending(), [])
  const stream = useMemo(
    () =>
      new TileStream(client, {
        put: (tx, ty, bytes, version) => void buffer.put(tx, ty, bytes, version),
        drop: (tx, ty) => buffer.drop(tx, ty),
      }),
    [client, buffer],
  )
  const pendingVersion = useSyncExternalStore(
    (listener) => pending.subscribe(listener),
    () => pending.snapshot(),
  )
  const [tool, setTool] = useState<Tool>('pencil')
  const [color, setColor] = useState(1)
  const [hover, setHover] = useState<Point | null>(null)
  const [committing, setCommitting] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [stamp, setStamp] = useState<Stamp | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const previousToolRef = useRef<Tool>('pencil')

  useEffect(
    () => () => {
      if (timerRef.current) clearTimeout(timerRef.current)
      void stream.close()
    },
    [stream],
  )

  const onViewport = useCallback(
    (viewport: Viewport) => {
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => {
        stream.show(tilesInRect(viewport.left, viewport.top, viewport.right, viewport.bottom)).catch((error: unknown) => {
          setMessage(`Live updates failed: ${error instanceof Error ? error.message : String(error)}`)
        })
      }, VIEWPORT_DEBOUNCE_MS)
    },
    [stream],
  )

  const onSelectTool = useCallback((next: Tool) => {
    setTool((current) => {
      if (next === 'eyedropper' && current !== 'eyedropper') previousToolRef.current = current
      return next
    })
  }, [])

  const onStroke = useCallback(
    (points: Point[]) => {
      setNotice(null)
      for (const point of points) {
        if (!inCanvas(point.x, point.y)) continue
        if (tool === 'eraser') pending.remove(point.x, point.y)
        else if (!pending.add({ ...point, color })) return
      }
    },
    [pending, tool, color],
  )

  const onShape = useCallback(
    (points: Point[]) => {
      setNotice(null)
      const pixels = points.filter((point) => inCanvas(point.x, point.y) && !pending.get(point.x, point.y))
      if (!pending.addAll(points.map((point) => ({ ...point, color }))))
        setNotice(`Needs ${pixels.length} px, ${MAX_PENDING - pending.size} left.`)
    },
    [pending, color],
  )

  const onPick = useCallback(
    (point: Point) => {
      const picked = pending.get(point.x, point.y)?.color ?? buffer.color(point.x, point.y)
      if (picked !== undefined) setColor(picked)
      setTool(previousToolRef.current)
    },
    [pending, buffer],
  )

  const onFile = useCallback((file: File) => {
    setMessage(null)
    loadStamp(file).then(setStamp, (error: unknown) => {
      setMessage(`Image failed: ${error instanceof Error ? error.message : String(error)}`)
    })
  }, [])

  const onPlace = useCallback(
    (point: Point) => {
      if (!stamp) {
        setNotice('Pick an image first (U).')
        return
      }
      setNotice(null)
      if (!pending.replace(stampPixels(stamp, point))) setMessage('Image could not be placed.')
    },
    [pending, stamp],
  )

  const ghost = useMemo(
    () => (tool === 'image' && stamp && hover ? stampPixels(stamp, hover) : null),
    [tool, stamp, hover],
  )

  const onCommit = async () => {
    setCommitting(true)
    setMessage(null)
    setNotice(null)
    const result = await commitPending(client, pending)
    setCommitting(false)
    if (!result.ok) setMessage(`Commit failed, your pixels are kept: ${result.message}`)
  }

  const count = pending.size
  void pendingVersion

  return (
    <div className="relative min-h-0 flex-1">
      <CanvasView
        buffer={buffer}
        pending={pending}
        tool={tool}
        color={color}
        onStroke={onStroke}
        onShape={onShape}
        onPick={onPick}
        onViewport={onViewport}
        onHover={setHover}
        ghost={ghost}
        onPlace={onPlace}
      />
      <Toolbar tool={tool} onSelect={onSelectTool} onFile={onFile} />
      <BottomBar
        color={color}
        onColor={setColor}
        hover={hover}
        count={count}
        committing={committing}
        onCommit={onCommit}
        onClear={() => {
          setNotice(null)
          pending.clear()
        }}
        notice={notice}
        message={message}
      />
    </div>
  )
}

async function loadStamp(file: File): Promise<Stamp> {
  const source = await createImageBitmap(file)
  const size = fitSize(source.width, source.height)
  const shrink = preShrinkSize(source.width, source.height)
  const bitmap = shrink
    ? await createImageBitmap(file, { resizeWidth: shrink.width, resizeHeight: shrink.height, resizeQuality: 'high' })
    : source
  if (bitmap !== source) source.close()
  const canvas = document.createElement('canvas')
  canvas.width = bitmap.width
  canvas.height = bitmap.height
  const context = canvas.getContext('2d')
  if (!context) throw new Error('2d canvas context unavailable')
  context.drawImage(bitmap, 0, 0)
  bitmap.close()
  const rgba = context.getImageData(0, 0, canvas.width, canvas.height).data
  return convert(rgba, canvas.width, canvas.height, size)
}
