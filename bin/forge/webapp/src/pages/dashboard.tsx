// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { Link } from 'react-router-dom'
import { useQueryExecutor } from '@reifydb/react'
import { StatusBadge, EmptyState, Button } from '@/components/ui'
import { cn } from '@/lib'
import { useEffect } from 'react'
import { useLiveData } from '@/hooks/use-live-data'

export function DashboardPage() {
  const { data: runs } = useLiveData(
    'FROM forge::runs | SORT {started_at:DESC} | TAKE 20'
  )
  const { results: pipelineResults, query } = useQueryExecutor<any>()

  useEffect(() => {
    query('FROM forge::pipelines')
  }, [query])

  const totalPipelines = pipelineResults?.[0]?.rows?.length ?? 0

  const stats = {
    total: runs.length,
    running: runs.filter((r: any) => r.status === 'running').length,
    succeeded: runs.filter((r: any) => r.status === 'succeeded').length,
    failed: runs.filter((r: any) => r.status === 'failed').length,
  }

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
          <span className="text-primary">$</span> forge status
        </div>
        <h1 className="text-2xl sm:text-3xl font-black tracking-tight">Dashboard</h1>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard label="# pipelines" value={totalPipelines} />
        <StatCard label="# running" value={stats.running} color="text-status-info" />
        <StatCard label="# succeeded" value={stats.succeeded} color="text-status-success" />
        <StatCard label="# failed" value={stats.failed} color="text-status-error" />
      </div>

      {/* Recent Runs */}
      <div className="mb-4">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-bold">Recent Runs</h2>
          <Link to="/pipelines" className="text-xs font-mono text-text-muted hover:text-primary transition-colors">
            [view all pipelines]
          </Link>
        </div>

        {runs.length === 0 ? (
          <EmptyState
            icon="~"
            title="No runs yet"
            description="Create a pipeline and trigger a run to see results here."
          >
            <Button href="/pipelines" size="sm">View Pipelines</Button>
          </EmptyState>
        ) : (
          <div className="border border-dashed border-black/25 overflow-hidden">
            <table className="w-full text-sm font-mono">
              <thead>
                <tr className="border-b border-dashed border-black/25 bg-bg-secondary">
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Run</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Pipeline</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Status</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Triggered</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Started</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run: any) => (
                  <tr
                    key={String(run.id)}
                    className="border-b border-dashed border-black/10 cursor-pointer hover:bg-bg-secondary transition-colors"
                  >
                    <td className="px-4 py-3">
                      <Link to={`/runs/${run.id}`} className="text-primary hover:underline">
                        {String(run.id).slice(0, 8)}...
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-text-secondary">
                      {String(run.pipeline_id).slice(0, 8)}...
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={String(run.status)} />
                    </td>
                    <td className="px-4 py-3 text-text-secondary">{String(run.triggered_by)}</td>
                    <td className="px-4 py-3 text-text-muted">{String(run.started_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

function StatCard({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="border border-dashed border-black/25 p-4">
      <div className="text-xs font-mono text-text-muted mb-1">{label}</div>
      <div className={cn('text-2xl font-bold font-mono', color ?? 'text-text-primary')}>
        {value}
      </div>
    </div>
  )
}
