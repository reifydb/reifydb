// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useProbes } from '@/hooks/use-probes'
import { RelativeTime } from '@/components/relative-time'
import {
  Card,
  EmptyState,
  Loading,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@reifydb/ui'

const ONLINE_WINDOW_MS = 30_000

export function ProbesPage() {
  const { data: probes, isLoading, error } = useProbes()

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl">Probes</h1>
      </div>

      {isLoading && <Loading />}
      {error != null && (
        <p className="text-sm text-status-error">Failed to load probes: {error.message}</p>
      )}

      {probes != null && probes.length === 0 && (
        <Card>
          <EmptyState
            title="No probes registered"
            description="Probes claim checks off the queue and report results back."
          />
        </Card>
      )}

      {probes != null && probes.length > 0 && (
        <div className="glass-card overflow-hidden">
          <Table>
            <TableHead>
              <TableHeader>Probe</TableHeader>
              <TableHeader>State</TableHeader>
              <TableHeader>Last seen</TableHeader>
              <TableHeader>ID</TableHeader>
            </TableHead>
            <TableBody>
              {probes.map((p) => {
                const online = Date.now() - Date.parse(p.last_seen) < ONLINE_WINDOW_MS
                return (
                  <TableRow key={p.id}>
                    <TableCell className="font-mono font-medium text-text-primary">
                      {p.name}
                    </TableCell>
                    <TableCell>
                      <span
                        className={`inline-flex items-center gap-1.5 font-mono text-xs uppercase tracking-wide ${
                          online ? 'text-status-success' : 'text-text-muted'
                        }`}
                      >
                        <span
                          className={`h-2 w-2 rounded-full ${
                            online ? 'bg-status-success' : 'bg-text-muted'
                          }`}
                        />
                        {online ? 'Online' : 'Offline'}
                      </span>
                    </TableCell>
                    <TableCell className="text-text-muted">
                      <RelativeTime iso={p.last_seen} />
                    </TableCell>
                    <TableCell className="font-mono text-xs text-text-muted">{p.id}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}
