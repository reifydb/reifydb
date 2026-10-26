// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import type { PlaceClient } from '@/client'
import { commitPending, commitPixels } from '@/commit'
import { MAX_PENDING, inCanvas } from '@/geometry'
import { type Stamp, convert, fitSize, preShrinkSize, stampPixels } from '@/image'
import { Minimap } from '@/minimap'
import type { Pixel } from '@/pack'
import { Pending } from '@/pending'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'
import type { Point, Tool } from '@/tools'
import { BottomBar } from './bottom-bar'
import { CanvasView } from './canvas-view'
import { Toolbar } from './toolbar'

export function Place({ client }: { client: PlaceClient }) {
  const buffer = useMemo(() => new PixelBuffer(), [])
  const minimap = useMemo(() => new Minimap(), [])
  const pending = useMemo(() => new Pending(), [])
  const stream = useMemo(
    () =>
      new TileStream(
        client,
        {
          put: (tx, ty, bytes, version) => {
            if (buffer.put(tx, ty, bytes, version)) minimap.put(tx, ty, bytes)
          },
        },
        () => setMessage('Live updates failed: the tile stream was closed.'),
      ),
    [client, buffer, minimap],
  )
  const pendingVersion = useSyncExternalStore(
    (listener) => pending.subscribe(listener),
    () => pending.snapshot(),
  )
  const [tool, setTool] = useState<Tool>('pan')
  const [color, setColor] = useState(1)
  const [layerVisible, setLayerVisible] = useState(true)
  const [hover, setHover] = useState<Point | null>(null)
  const [committing, setCommitting] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [stamp, setStamp] = useState<Stamp | null>(null)
  const [placing, setPlacing] = useState<Pixel[] | null>(null)
  const previousToolRef = useRef<Tool>('pencil')
  const imageReturnRef = useRef<Tool>('pencil')

  useEffect(() => {
    stream.start().catch((error: unknown) => {
      setMessage(`Live updates failed: ${error instanceof Error ? error.message : String(error)}`)
    })
    return () => void stream.close()
  }, [stream])

  const onSelectTool = useCallback((next: Tool) => {
    setTool((current) => {
      if (next === 'eyedropper' && current !== 'eyedropper') previousToolRef.current = current
      if (next === 'image' && current !== 'image') imageReturnRef.current = current === 'eyedropper' ? previousToolRef.current : current
      return next
    })
  }, [])

  const onStroke = useCallback(
    (points: Point[]) => {
      setNotice(null)
      setLayerVisible(true)
      for (const point of points) {
        if (!inCanvas(point.x, point.y)) continue
        if (!pending.add({ ...point, color })) return
      }
    },
    [pending, color],
  )

  const onShape = useCallback(
    (points: Point[]) => {
      setNotice(null)
      setLayerVisible(true)
      const pixels = points.filter((point) => inCanvas(point.x, point.y) && !pending.get(point.x, point.y))
      if (!pending.addAll(points.map((point) => ({ ...point, color }))))
        setNotice(`Needs ${pixels.length} px, your layer has ${MAX_PENDING - pending.size} left.`)
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

  const onFile = useCallback(
    (file: File) => {
      setMessage(null)
      loadStamp(file).then(
        async (loaded) => {
          setCommitting(true)
          const result = await commitPending(client, pending)
          setCommitting(false)
          if (!result.ok) {
            setMessage(`Merge failed, your layer is kept: ${result.message}`)
            setTool(imageReturnRef.current)
            return
          }
          setStamp(loaded)
        },
        (error: unknown) => {
          setMessage(`Image failed: ${error instanceof Error ? error.message : String(error)}`)
        },
      )
    },
    [client, pending],
  )

  const onPlace = useCallback(
    (point: Point) => {
      if (placing) return
      if (!stamp) {
        setNotice('Pick an image first (U).')
        return
      }
      const pixels = stampPixels(stamp, point)
      const inside = pixels.filter((pixel) => inCanvas(pixel.x, pixel.y))
      if (inside.length === 0) return
      setNotice(null)
      setMessage(null)
      setPlacing(pixels)
      setCommitting(true)
      commitPixels(client, inside).then((result) => {
        setCommitting(false)
        setPlacing(null)
        if (!result.ok) {
          setMessage(`Image merge failed: ${result.message}`)
          return
        }
        setStamp(null)
        setTool((current) => (current === 'image' ? imageReturnRef.current : current))
      })
    },
    [client, stamp, placing],
  )

  const ghost = useMemo(
    () => placing ?? (tool === 'image' && stamp && hover ? stampPixels(stamp, hover) : null),
    [placing, tool, stamp, hover],
  )

  const onCommit = async () => {
    setCommitting(true)
    setMessage(null)
    setNotice(null)
    const result = await commitPending(client, pending)
    setCommitting(false)
    if (!result.ok) setMessage(`Merge failed, your layer is kept: ${result.message}`)
  }

  const count = pending.size
  void pendingVersion

  return (
    <div className="relative min-h-0 flex-1">
      <CanvasView
        buffer={buffer}
        minimap={minimap}
        pending={pending}
        layerVisible={layerVisible}
        tool={tool}
        color={color}
        onStroke={onStroke}
        onShape={onShape}
        onPick={onPick}
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
        layerVisible={layerVisible}
        onLayerVisible={setLayerVisible}
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
