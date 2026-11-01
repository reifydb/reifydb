// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type CSSProperties, useEffect, useState, useSyncExternalStore } from 'react'
import type { Activity, Edit, Streak } from '@/activity'
import { cssColor } from '@/palette'

export function useEdits(activity: Activity): readonly Edit[] {
  return useSyncExternalStore(
    (listener) => activity.subscribe(listener),
    () => activity.latest(),
  )
}

export function useStreaks(activity: Activity): readonly Streak[] {
  return useSyncExternalStore(
    (listener) => activity.subscribe(listener),
    () => activity.streaks(),
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

export function ago(now: number, at: number): string {
  const seconds = Math.max(0, Math.round((now - at) / 1000))
  return seconds < 2 ? 'now' : seconds < 60 ? `${seconds}s ago` : `${Math.round(seconds / 60)}m ago`
}

export function ActivityFeed({
  activity,
  onHover,
  onSelect,
}: {
  activity: Activity
  onHover: (streak: Streak | null) => void
  onSelect: (streak: Streak) => void
}) {
  const streaks = useStreaks(activity)
  const now = useNow(1000)
  if (streaks.length === 0) return <p className="px-3 py-4 text-sm text-text-muted">Waiting for the next edit...</p>
  return (
    <ul aria-live="polite">
      {streaks.map((streak) => (
        <li key={streak.id} className="activity-row border-b border-border-light">
          <button
            type="button"
            className="flex w-full flex-col gap-0.5 border-l-[3px] border-l-[var(--streak)] py-2 pl-2.5 pr-3 text-left text-xs transition-[border-width,padding,background-color] duration-150 hover:border-l-[6px] hover:bg-[color-mix(in_srgb,var(--streak)_8%,transparent)] hover:pl-[7px] focus-visible:border-l-[6px] focus-visible:bg-[color-mix(in_srgb,var(--streak)_8%,transparent)] focus-visible:pl-[7px] focus-visible:outline-none"
            style={{ '--streak': cssColor(streak.color) } as CSSProperties}
            title="Go to this paint"
            onPointerEnter={() => onHover(streak)}
            onPointerLeave={() => onHover(null)}
            onFocus={() => onHover(streak)}
            onBlur={() => onHover(null)}
            onClick={() => onSelect(streak)}
          >
            <span className="flex items-baseline justify-between gap-2">
              <span className="truncate font-semibold text-primary-dark">{streak.name}</span>
              <span className="shrink-0 tabular-nums">+{streak.px} px</span>
            </span>
            <span className="flex items-baseline justify-between gap-2 font-mono text-text-muted">
              <span>{streak.tiles.length === 1 ? `tile ${streak.tx},${streak.ty}` : `${streak.tiles.length} tiles`}</span>
              <span className="shrink-0">{ago(now, streak.at)}</span>
            </span>
          </button>
        </li>
      ))}
    </ul>
  )
}
