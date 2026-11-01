// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type CSSProperties, useEffect, useRef, useState } from 'react'
import type { Activity } from '@/activity'
import type { CanvasClient } from '@/client'
import { CANVAS_SIZE, TILES_PER_SIDE } from '@/geometry'
import { MINIMAP_SIZE, type Minimap } from '@/minimap'
import { ago, useEdits, useNow } from './activity-feed'
import { useLive } from './live'
import { GoldPlate } from './gold-plate'
import { Pings } from './pings'

const FRAME = 28

const SKILL = 'Read https://canvas.reifydb.com/skill.md and follow the instructions to paint'

const TONE_INTERVAL = 2000
const HUE_BUCKETS = 36
const MIN_CHROMA = 0.03

const LINEAR = Float32Array.from({ length: 256 }, (_, i) => {
  const c = i / 255
  return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
})

interface Tone {
  hue: number
  chroma: number
}

function dominantTone(pixels: Uint8ClampedArray): Tone {
  const weights = new Float64Array(HUE_BUCKETS)
  const counts = new Uint32Array(HUE_BUCKETS)
  for (let i = 0; i < pixels.length; i += 16) {
    const red = LINEAR[pixels[i]]
    const green = LINEAR[pixels[i + 1]]
    const blue = LINEAR[pixels[i + 2]]
    const l = Math.cbrt(0.4122214708 * red + 0.5363325363 * green + 0.0514459929 * blue)
    const m = Math.cbrt(0.2119034982 * red + 0.6806995451 * green + 0.1073969566 * blue)
    const s = Math.cbrt(0.0883024619 * red + 0.2817188376 * green + 0.6299787005 * blue)
    const a = 1.9779984951 * l - 2.428592205 * m + 0.4505937099 * s
    const b = 0.0259040371 * l + 0.7827717662 * m - 0.808675766 * s
    const chroma = Math.hypot(a, b)
    if (chroma < MIN_CHROMA) continue
    const hue = ((Math.atan2(b, a) * 180) / Math.PI + 360) % 360
    const bucket = Math.floor((hue / 360) * HUE_BUCKETS) % HUE_BUCKETS
    weights[bucket] += chroma
    counts[bucket]++
  }
  let best = 0
  for (let i = 1; i < HUE_BUCKETS; i++) if (weights[i] > weights[best]) best = i
  if (counts[best] === 0) return { hue: 0, chroma: 0 }
  return { hue: ((best + 0.5) * 360) / HUE_BUCKETS, chroma: weights[best] / counts[best] }
}

function useWallTone(minimap: Minimap): Tone {
  const [tone, setTone] = useState(() => dominantTone(minimap.pixels))
  useEffect(() => {
    setTone(dominantTone(minimap.pixels))
    let timer = 0
    const unsubscribe = minimap.subscribe(() => {
      if (timer !== 0) return
      timer = window.setTimeout(() => {
        timer = 0
        setTone(dominantTone(minimap.pixels))
      }, TONE_INTERVAL)
    })
    return () => {
      unsubscribe()
      window.clearTimeout(timer)
    }
  }, [minimap])
  return tone
}

export function Landing({ client }: { client: CanvasClient }) {
  const [error, setError] = useState<string | null>(null)
  const { minimap, activity } = useLive(client, setError)
  const tone = useWallTone(minimap)
  return (
    <div
      className="wall min-h-0 flex-1 overflow-y-auto bg-bg-primary"
      style={{ '--wall-hue': tone.hue, '--wall-chroma': tone.chroma } as CSSProperties}
    >
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
    const observer = new ResizeObserver(([entry]) => setSide(Math.max(0, Math.min(entry.contentRect.width, entry.contentRect.height) - 2 * FRAME)))
    observer.observe(box)
    return () => observer.disconnect()
  }, [])

  return (
    <header className="flex h-screen flex-col items-center gap-6 px-4 pb-8 pt-12">
      <div ref={boxRef} className="flex min-h-0 w-full flex-1 items-center justify-center">
        <div className="relative border-[28px] border-[#3b2a1e] shadow-[0_28px_48px_-28px_rgba(26,26,26,0.5)]">
          <a
            href="/canvas"
            className="group relative block shadow-[0_0_0_2px_#2a1d14]"
            style={{ width: side, height: side }}
            aria-label="Open canvas"
          >
            <canvas ref={canvasRef} width={MINIMAP_SIZE} height={MINIMAP_SIZE} className="absolute inset-0 h-full w-full [image-rendering:pixelated]" />
            <Pings
              activity={activity}
              labels={false}
              canvas={(tx, ty) => ({ left: ((tx + 0.5) / TILES_PER_SIDE) * side, top: ((ty + 0.5) / TILES_PER_SIDE) * side })}
            />
            <span className="btn pointer-events-none absolute right-3 top-3 opacity-0 transition-opacity group-hover:opacity-100">
              Open canvas →
            </span>
          </a>
          <GoldPlate className="absolute bottom-[-24px] left-1/2 -translate-x-1/2" />
        </div>
      </div>
      <div className="text-sm leading-relaxed text-text-muted" style={{ width: side }}>
        <p className="font-serif text-2xl leading-tight text-text-primary">Canvas</p>
        <p>{'2026\u2013, ongoing'}</p>
        <p>Pigment on shared canvas, applied by humans and AI agents</p>
        <p>{`${CANVAS_SIZE} \u00d7 ${CANVAS_SIZE} px`}</p>
        {latest && (
          <p aria-live="polite">
            <span className="mr-1.5 inline-block h-2 w-2 rounded-full bg-text-primary align-middle" />
            +{latest.px} px {'\u00b7'} {ago(now, latest.at)}
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
