// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useParams, Link } from 'react-router-dom'
import { useQueryExecutor, useCommandExecutor } from '@reifydb/react'
import { StatusBadge, Button } from '@/components/ui'
import { cn } from '@/lib'
import { useEffect } from 'react'
import { useLiveData } from '@/hooks/use-live-data'

export function RunDetailPage() {
  const { id } = useParams<{ id: string }>()
  const { results: runResults, query: queryRun } = useQueryExecutor<any>()
  const { data: jobRuns } = useLiveData(
    `FROM forge::job_runs | FILTER run_id == uuid::v4("${id}")`
  )
  const { data: stepRuns } = useLiveData(
    `FROM forge::step_runs | FILTER run_id == uuid::v4("${id}")`
  )
  const { data: jobs } = useLiveData(
    `FROM forge::jobs`
  )
  const { data: deps } = useLiveData(
    `FROM forge::job_dependencies`
  )
  const { data: logs } = useLiveData(
    `FROM forge::logs | FILTER run_id == uuid::v4("${id}") | SORT {line_number:ASC}`
  )

  const { command, isExecuting } = useCommandExecutor()

  useEffect(() => {
    if (id) queryRun(`FROM forge::runs | FILTER id == uuid::v4("${id}")`)
  }, [id, queryRun])

  const cancelRun = () => {
    command(`CALL forge::cancel_run(uuid::v4("${id}"))`)
  }

  const runData = runResults?.[0]?.rows?.[0] as any
  const canCancel = runData?.status === 'pending' || runData?.status === 'running'

  // Build a map of job_id -> job name
  const jobNameMap: Record<string, string> = {}
  for (const job of jobs) {
    jobNameMap[String(job.id)] = String(job.name)
  }

  // Build dependency map: job_id -> [depends_on_job_id]
  const depsMap = new Map<string, string[]>()
  for (const dep of deps) {
    const jobId = String(dep.job_id)
    if (!depsMap.has(jobId)) depsMap.set(jobId, [])
    depsMap.get(jobId)!.push(String(dep.depends_on_job_id))
  }

  // Group step_runs by job_run_id
  const stepRunsByJobRun: Record<string, any[]> = {}
  for (const sr of stepRuns) {
    const jrId = String(sr.job_run_id)
    if (!stepRunsByJobRun[jrId]) stepRunsByJobRun[jrId] = []
    stepRunsByJobRun[jrId].push(sr)
  }

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
        <span className="text-primary">$</span>
        <Link to="/" className="hover:text-primary transition-colors">dashboard</Link>
        <span>/</span>
        <span className="text-text-secondary">run {id?.slice(0, 8)}</span>
      </div>

      {/* Header */}
      <div className="flex items-start justify-between mb-8">
        <div>
          <h1 className="text-2xl sm:text-3xl font-black tracking-tight">
            Run {id?.slice(0, 8)}...
          </h1>
          <div className="flex items-center gap-3 mt-2">
            {runData?.status && <StatusBadge status={String(runData.status)} />}
            <span className="text-xs font-mono text-text-muted">
              triggered by {String(runData?.triggered_by ?? 'unknown')}
            </span>
          </div>
        </div>
        {canCancel && (
          <Button onClick={cancelRun} disabled={isExecuting} variant="secondary" size="sm">
            {isExecuting ? 'Cancelling...' : 'Cancel'}
          </Button>
        )}
      </div>

      {/* Job Runs */}
      <div className="mb-8">
        <h2 className="text-lg font-bold mb-4">Jobs</h2>
        {jobRuns.length === 0 ? (
          <div className="text-sm text-text-muted font-mono">No jobs found for this run.</div>
        ) : (
          <div className="grid gap-4">
            {jobRuns.map((jr: any) => {
              const jobName = jobNameMap[String(jr.job_id)] ?? String(jr.job_id).slice(0, 8)
              const nested = stepRunsByJobRun[String(jr.id)] ?? []

              return (
                <div key={String(jr.id)} className="border border-dashed border-black/25">
                  {/* Job run header */}
                  <div className="px-4 py-3 flex items-center justify-between border-b border-dashed border-black/10 bg-bg-secondary">
                    <div className="flex items-center gap-3">
                      <span className="font-bold text-sm">{jobName}</span>
                      <StatusBadge status={String(jr.status)} />
                      {(() => {
                        const depIds = depsMap.get(String(jr.job_id)) ?? []
                        const depNames = depIds.map(d => jobNameMap[d]).filter(Boolean)
                        return depNames.length > 0 ? (
                          <span className="text-xs font-mono text-text-muted">
                            needs: {depNames.join(', ')}
                          </span>
                        ) : null
                      })()}
                    </div>
                    <div className="flex items-center gap-4 text-xs font-mono text-text-muted">
                      {jr.started_at && <span>started: {String(jr.started_at)}</span>}
                      {jr.finished_at && <span>finished: {String(jr.finished_at)}</span>}
                    </div>
                  </div>

                  {/* Nested step runs */}
                  {nested.length > 0 && (
                    <div className="divide-y divide-dashed divide-black/10">
                      {nested.map((sr: any) => (
                        <div
                          key={String(sr.id)}
                          className="px-4 py-2.5 pl-8 flex items-center justify-between"
                        >
                          <div className="flex items-center gap-3">
                            <span className="text-xs font-mono text-text-muted">{String(sr.id).slice(0, 8)}</span>
                            <StatusBadge status={String(sr.status)} />
                          </div>
                          <div className="flex items-center gap-4 text-xs font-mono text-text-muted">
                            {sr.exit_code != null && (
                              <span>exit: {String(sr.exit_code)}</span>
                            )}
                            {sr.started_at && <span>started: {String(sr.started_at)}</span>}
                            {sr.finished_at && <span>finished: {String(sr.finished_at)}</span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>

      {/* Logs */}
      <div>
        <h2 className="text-lg font-bold mb-4">Logs</h2>
        {logs.length === 0 ? (
          <div className="text-sm text-text-muted font-mono">No logs yet.</div>
        ) : (
          <div className="bg-code-bg border border-code-border overflow-hidden">
            <div className="px-4 py-2 border-b border-code-border flex items-center gap-2">
              <span className="text-xs font-mono text-code-text-muted">output</span>
              <span className="text-xs font-mono text-code-text-muted">({logs.length} lines)</span>
            </div>
            <div className="p-4 max-h-[600px] overflow-y-auto font-mono text-sm">
              {logs.map((log: any, i: number) => (
                <div key={i} className="flex gap-3 leading-relaxed">
                  <span className="text-code-text-muted select-none w-8 text-right shrink-0">
                    {String(log.line_number)}
                  </span>
                  <span className={cn(
                    log.stream === 'stderr' ? 'text-status-error' : 'text-code-text',
                  )}>
                    {String(log.line)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
