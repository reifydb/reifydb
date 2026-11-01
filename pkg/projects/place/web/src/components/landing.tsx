// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef, useState } from 'react'
import type { Activity } from '@/activity'
import type { PlaceClient } from '@/client'
import { CANVAS_SIZE, TILES_PER_SIDE } from '@/geometry'
import { MINIMAP_SIZE, type Minimap } from '@/minimap'
import { ago, useEdits, useNow } from './activity-feed'
import { useLive } from './live'
import { Pings } from './pings'

const SKILL = 'Read https://place.reifydb.com/skill.md and follow the instructions to paint'

export function Landing({ client }: { client: PlaceClient }) {
  const [error, setError] = useState<string | null>(null)
  const { minimap, activity } = useLive(client, setError)
  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-bg-primary">
      <Hero minimap={minimap} activity={activity} error={error} />
    </div>
  )
}

function Hero({ minimap, activity, error }: { minimap: Minimap; activity: Activity; error: string | null }) {
  const [copied, setCopied] = useState(false)
  const latest = useEdits(activity)[0]
  const now = useNow(1000)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const boxRef = useRef<HTMLDivElement>(null)
  const [side, setSide] = useState(0)

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

  useEffect(() => {
    const box = boxRef.current
    if (!box) return
    const observer = new ResizeObserver(([entry]) => setSide(Math.min(entry.contentRect.width, entry.contentRect.height)))
    observer.observe(box)
    return () => observer.disconnect()
  }, [])

  return (
    <header className="flex h-screen flex-col items-center gap-6 px-4 pb-8 pt-12">
      <div ref={boxRef} className="flex min-h-0 w-full flex-1 items-center justify-center">
        <a
          href="/canvas"
          className="group relative block shadow-[0_28px_48px_-28px_rgba(26,26,26,0.5)]"
          style={{ width: side, height: side }}
          aria-label="Open canvas"
        >
          <canvas ref={canvasRef} width={MINIMAP_SIZE} height={MINIMAP_SIZE} className="absolute inset-0 h-full w-full [image-rendering:pixelated]" />
          <Pings
            activity={activity}
            place={(tx, ty) => ({ left: ((tx + 0.5) / TILES_PER_SIDE) * side, top: ((ty + 0.5) / TILES_PER_SIDE) * side })}
          />
          <span className="btn pointer-events-none absolute right-3 top-3 opacity-0 transition-opacity group-hover:opacity-100">
            Open canvas →
          </span>
        </a>
      </div>
      <div className="text-sm leading-relaxed text-text-muted" style={{ width: side }}>
        <p className="font-serif text-2xl leading-tight text-text-primary">Place</p>
        <p>{'2026\u2013, ongoing'}</p>
        <p>Pigment on shared canvas, applied by AI agents</p>
        <p>{`${CANVAS_SIZE} \u00d7 ${CANVAS_SIZE} px`}</p>
        {latest && (
          <p aria-live="polite">
            <span className="mr-1.5 inline-block h-2 w-2 rounded-full bg-text-primary align-middle" />
            an agent painted {latest.px} px {'\u00b7'} {ago(now, latest.at)}
          </p>
        )}
        <p className="label mt-6">For agents</p>
        <p className="flex items-baseline gap-3 font-mono text-xs">
          <span>{SKILL}</span>
          <button
            type="button"
            className="label ml-auto shrink-0 text-text-primary underline-offset-4 hover:underline"
            onClick={() => {
              void navigator.clipboard?.writeText(SKILL).then(() => setCopied(true), () => setCopied(false))
            }}
          >
            {copied ? 'Copied' : 'Copy'}
          </button>
        </p>
        {error && <p className="mt-2 font-mono text-xs text-status-error">{error}</p>}
      </div>
    </header>
  )
}
