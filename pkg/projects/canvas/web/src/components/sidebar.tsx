// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState } from 'react'
import type { Activity, Streak } from '@/activity'
import type { Minimap } from '@/minimap'
import { ActivityFeed } from './activity-feed'
import type { Camera } from './canvas-view'
import { MinimapPanel } from './minimap-panel'
import { tileCenter } from './pings'
import { ZoomControl } from './zoom-control'

export function Sidebar({
  activity,
  minimap,
  camera,
  onHover,
}: {
  activity: Activity
  minimap: Minimap
  camera: Camera
  onHover: (streak: Streak | null) => void
}) {
  const [open, setOpen] = useState(true)
  if (!open) {
    return (
      <button
        type="button"
        className="hidden w-8 shrink-0 border-l border-border-light bg-white text-primary-dark md:block"
        title="Show sidebar"
        aria-label="Show sidebar"
        onClick={() => setOpen(true)}
      >
        ⟨
      </button>
    )
  }
  return (
    <aside className="hidden w-[220px] shrink-0 flex-col border-l border-border-light bg-white md:flex" aria-label="Sidebar">
      <div className="flex justify-center border-b border-border-light py-2">
        <MinimapPanel
          minimap={minimap}
          view={camera.view}
          size={camera.size}
          onCenter={camera.centerOn}
          footer={<ZoomControl zoom={camera.view.zoom} onZoomIn={camera.zoomIn} onZoomOut={camera.zoomOut} onReset={camera.reset} />}
        />
      </div>
      <h2 className="flex items-center justify-between border-b border-border-light px-3 py-2 text-sm font-bold">
        Activity
      </h2>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <ActivityFeed
          activity={activity}
          onHover={onHover}
          onSelect={(streak) => camera.centerOn(tileCenter(streak.tx, streak.ty))}
        />
      </div>
      <button type="button" className="border-t border-border-light px-3 py-2 text-left text-xs text-text-muted" onClick={() => setOpen(false)}>
        ⟩ hide sidebar
      </button>
    </aside>
  )
}
