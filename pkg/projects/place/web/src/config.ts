// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

declare global {
  interface Window {
    REIFYDB_WS_URL?: string
  }
}

export function wsUrl(): string {
  if (window.REIFYDB_WS_URL) return window.REIFYDB_WS_URL
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${proto}://${window.location.hostname}:8091`
}
