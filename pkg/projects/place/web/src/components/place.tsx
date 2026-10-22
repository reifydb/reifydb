// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import { Button, Card, CardContent, CardHeader, CardTitle } from '@reifydb/ui'
import type { PlaceClient } from '@/client'
import { commitPending } from '@/commit'
import { MAX_PENDING, tilesInRect } from '@/geometry'
import { cssColor } from '@/palette'
import { Pending } from '@/pending'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'
import { CanvasView, type Viewport } from './canvas-view'
import { PalettePicker } from './palette-picker'

const VIEWPORT_DEBOUNCE_MS = 150

export function Place({ client }: { client: PlaceClient }) {
  const buffer = useMemo(() => new PixelBuffer(), [])
  const pending = useMemo(() => new Pending(), [])
  const stream = useMemo(() => new TileStream(client, (x, y, color) => buffer.set(x, y, color)), [client, buffer])
  const pendingVersion = useSyncExternalStore(
    (listener) => pending.subscribe(listener),
    () => pending.snapshot(),
  )
  const [color, setColor] = useState(1)
  const [hover, setHover] = useState<{ x: number; y: number } | null>(null)
  const [committing, setCommitting] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

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

  const onPaint = useCallback(
    (x: number, y: number) => {
      if (!pending.add({ x, y, color })) setMessage(`A commit holds at most ${MAX_PENDING} pixels. Commit or clear first.`)
    },
    [pending, color],
  )

  const onCommit = async () => {
    setCommitting(true)
    setMessage(null)
    const result = await commitPending(client, pending)
    setCommitting(false)
    if (!result.ok) setMessage(`Commit failed, your pixels are kept: ${result.message}`)
  }

  const count = pending.size
  void pendingVersion

  return (
    <div className="flex flex-1 flex-col gap-6 lg:flex-row">
      <div className="flex min-h-[480px] flex-1">
        <CanvasView buffer={buffer} pending={pending} onPaint={onPaint} onViewport={onViewport} onHover={setHover} />
      </div>
      <aside className="flex w-full flex-col gap-4 lg:w-80">
        <Card>
          <CardHeader>
            <CardTitle className="font-mono text-xs uppercase tracking-[1.4px]">Color</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <div className="flex items-center gap-3">
              <span className="h-8 w-8 border-2 border-border-default" style={{ backgroundColor: cssColor(color) }} />
              <span className="font-mono text-xs text-text-secondary">
                #{color} {cssColor(color)}
              </span>
            </div>
            <PalettePicker selected={color} onSelect={setColor} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="font-mono text-xs uppercase tracking-[1.4px]">Pending</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <p className="font-mono text-sm text-text-primary">
              {count} / {MAX_PENDING} pixels
            </p>
            <div className="flex gap-2">
              <Button onClick={onCommit} disabled={count === 0 || committing}>
                {committing ? 'Committing' : `Commit ${count}`}
              </Button>
              <Button variant="secondary" onClick={() => pending.clear()} disabled={count === 0 || committing}>
                Clear
              </Button>
            </div>
            {message && <p className="font-mono text-xs text-status-error">{message}</p>}
          </CardContent>
        </Card>
        <p className="font-mono text-xs uppercase tracking-[1.4px] text-text-muted">
          {hover ? `x ${hover.x}  y ${hover.y}` : 'Click to paint, drag to pan, scroll to zoom'}
        </p>
      </aside>
    </div>
  )
}
