// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useSyncExternalStore } from 'react'

const subscribers = new Set<() => void>()
let timer: ReturnType<typeof setInterval> | null = null
let now = Date.now()

function subscribe(callback: () => void): () => void {
  subscribers.add(callback)
  if (timer == null) {
    now = Date.now()
    timer = setInterval(() => {
      now = Date.now()
      for (const notify of subscribers) notify()
    }, 1000)
  }
  return () => {
    subscribers.delete(callback)
    if (subscribers.size === 0 && timer != null) {
      clearInterval(timer)
      timer = null
    }
  }
}

function getSnapshot(): number {
  return now
}

export function useNow(): number {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}
