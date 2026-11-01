// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type CanvasClient, numeric } from './client'

export const AGENTS_RQL = `from canvas::edit
  map { owner, name, painted, at: datetime::epoch_millis(#created_at) }
  aggregate { painted: math::sum(painted), last: math::max(at) } by { owner, name }`

export interface PainterRow {
  owner: unknown
  name: unknown
  painted: unknown
  last: unknown
}

export interface Painter {
  owner: string
  name: string
  painted: number
  last: number
}

export interface Agents {
  painters: Painter[]
}

function text(value: unknown): string {
  if (value !== null && typeof value === 'object' && 'value' in value) return String((value as { value: unknown }).value)
  return String(value)
}

export function summarize(rows: readonly PainterRow[]): Agents {
  const byOwner = new Map<string, Painter>()
  for (const row of rows) {
    const owner = text(row.owner)
    const name = text(row.name)
    const painted = numeric(row.painted)
    const last = numeric(row.last)
    const seen = byOwner.get(owner)
    if (!seen) {
      byOwner.set(owner, { owner, name, painted, last })
      continue
    }
    seen.painted += painted
    if (last > seen.last) {
      seen.last = last
      seen.name = name
    }
  }
  const painters = [...byOwner.values()].sort((a, b) => b.painted - a.painted || b.last - a.last)
  return { painters }
}

export async function loadAgents(client: CanvasClient): Promise<Agents> {
  const frames = (await client.query(AGENTS_RQL, {}, [])) as PainterRow[][]
  return summarize(frames[0] ?? [])
}
