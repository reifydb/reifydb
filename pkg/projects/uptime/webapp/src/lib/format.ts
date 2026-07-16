// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString()
}

export function formatRelativeTime(iso: string | null): string {
  if (iso == null) return 'never'
  const seconds = Math.round((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 5) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export function formatLatency(ms: number | null): string {
  if (ms == null) return '-'
  return `${ms} ms`
}
