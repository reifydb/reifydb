// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, useSyncExternalStore } from 'react'
import type { Activity, Edit } from '@/activity'
import { cssColor } from '@/palette'
import { Robot } from './sprites'

export function useEdits(activity: Activity): readonly Edit[] {
  return useSyncExternalStore(
    (listener) => activity.subscribe(listener),
    () => activity.latest(),
  )
}

export function useNow(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(timer)
  }, [intervalMs])
  return now
}

function ago(now: number, at: number): string {
  const seconds = Math.max(0, Math.round((now - at) / 1000))
  return seconds < 2 ? 'now' : seconds < 60 ? `${seconds}s ago` : `${Math.round(seconds / 60)}m ago`
}

export function ActivityFeed({ activity, rows }: { activity: Activity; rows: number }) {
  const edits = useEdits(activity).slice(0, rows)
  const now = useNow(1000)
  if (edits.length === 0) return <p className="px-3 py-4 text-sm text-text-muted">Waiting for the next edit...</p>
  return (
    <ul className="overflow-hidden" aria-live="polite">
      {edits.map((edit, index) => (
        <li key={edit.id} className="activity-row grid grid-cols-[18px_1fr] items-center gap-2 border-b border-border-light px-3 py-2 text-xs">
          <Robot blinking={index === 0} />
          <span>
            <span className="font-semibold text-primary-dark">An agent</span> painted {edit.px} px{' '}
            <span className="inline-block h-2.5 w-2.5 border border-border-light align-middle" style={{ backgroundColor: cssColor(edit.color) }} />
            <br />
            <span className="font-mono text-text-muted">
              tile ({edit.tx}, {edit.ty}) · {ago(now, edit.at)}
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}
