// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Activity } from '@/activity'
import { TILE_SIZE } from '@/geometry'
import { useEdits, useNow } from './activity-feed'
import { Robot } from './sprites'

export const PING_MS = 2600

export function Pings({ activity, place }: { activity: Activity; place: (tx: number, ty: number) => { left: number; top: number } | null }) {
  const now = useNow(250)
  const edits = useEdits(activity).filter((edit) => now - edit.at < PING_MS)
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden" aria-hidden="true">
      {edits.map((edit) => {
        const at = place(edit.tx, edit.ty)
        if (!at) return null
        return (
          <div key={edit.id} style={{ left: at.left, top: at.top }} className="absolute">
            <span className="ping-dot" />
            <span className="ping-card panel">
              <Robot blinking />+{edit.px} px
            </span>
          </div>
        )
      })}
    </div>
  )
}

export function tileCenter(tx: number, ty: number): { x: number; y: number } {
  return { x: tx * TILE_SIZE + TILE_SIZE / 2, y: ty * TILE_SIZE + TILE_SIZE / 2 }
}
