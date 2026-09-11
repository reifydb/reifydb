// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMemo } from 'react'
import { Shape, useSubscription, type InferShape, type SubscriptionConfig } from '@reifydb/react'
import type { Probe } from '@/lib/types'

export const PROBES_RQL = 'from uptime::probes'

export const probeShape = Shape.object({
  id: Shape.identityid(),
  name: Shape.utf8(),
  last_seen: Shape.datetime(),
})

type ProbeRow = InferShape<typeof probeShape>

const PROBES_CONFIG: SubscriptionConfig = { hydration: { enabled: true, maxRows: 1000 } }

function toProbe(row: ProbeRow): Probe {
  return { id: row.id, name: row.name, last_seen: row.lastSeen.toISOString() }
}

function useProbeRows() {
  return useSubscription(PROBES_RQL, null, probeShape, { config: PROBES_CONFIG })
}

export function useProbes(): { data: Probe[] | undefined; isLoading: boolean; error: Error | undefined } {
  const entry = useProbeRows()
  const data = useMemo(
    () =>
      entry.status === 'ready'
        ? entry.data.map(toProbe).sort((a, b) => a.name.localeCompare(b.name))
        : undefined,
    [entry.status, entry.data],
  )
  return { data, isLoading: entry.status === 'loading', error: entry.error }
}

export function useProbeNames(): { data: Record<string, string>; error: Error | undefined } {
  const entry = useProbeRows()
  const data = useMemo(() => {
    const names: Record<string, string> = {}
    for (const probe of entry.data) names[probe.id] = probe.name
    return names
  }, [entry.data])
  return { data, error: entry.error }
}
