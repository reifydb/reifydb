import { useParams, Link } from 'react-router-dom'
import { useQueryExecutor } from '@reifydb/react'
import { StatusBadge, EmptyState } from '@/components/ui'
import { useEffect } from 'react'
import { useLiveData } from '@/hooks/use-live-data'

function formatDuration(startedAt: string, finishedAt: string): string {
  const start = new Date(startedAt).getTime()
  const end = new Date(finishedAt).getTime()
  const diffMs = end - start
  if (isNaN(diffMs) || diffMs < 0) return '—'
  const totalSec = Math.floor(diffMs / 1000)
  const min = Math.floor(totalSec / 60)
  const sec = totalSec % 60
  return `${min}m ${sec.toString().padStart(2, '0')}s`
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr)
  if (isNaN(d.getTime())) return String(dateStr)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) +
    ' ' + d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
}

export function JobDetailPage() {
  const { id } = useParams<{ id: string }>()
  const { results: jobResults, query: queryJob } = useQueryExecutor<any>()
  const { data: steps } = useLiveData(
    `FROM forge::steps | FILTER job_id == uuid::v4("${id}") | SORT {position:ASC}`
  )
  const { data: deps } = useLiveData(
    `FROM forge::job_dependencies`
  )
  const { data: allJobs } = useLiveData(
    `FROM forge::jobs`
  )
  const { data: jobRuns } = useLiveData(
    `FROM forge::job_runs | FILTER job_id == uuid::v4("${id}") | SORT {started_at:DESC} | TAKE 10`
  )
  const { data: pipelines } = useLiveData(
    `FROM forge::pipelines`
  )

  useEffect(() => {
    if (id) queryJob(`FROM forge::jobs | FILTER id == uuid::v4("${id}")`)
  }, [id, queryJob])

  const jobData = jobResults?.[0]?.rows?.[0] as any

  // Job name lookup
  const jobNameById: Record<string, string> = {}
  for (const job of allJobs) jobNameById[String(job.id)] = String(job.name)

  // Pipeline name lookup
  const pipelineNameById: Record<string, string> = {}
  for (const p of pipelines) pipelineNameById[String(p.id)] = String(p.name)

  const pipelineId = jobData ? String(jobData.pipeline_id) : null
  const pipelineName = pipelineId ? pipelineNameById[pipelineId] : null

  // Dependencies: jobs this job needs
  const needs: string[] = []
  // Reverse dependencies: jobs that depend on this job
  const requiredBy: string[] = []

  for (const dep of deps) {
    if (String(dep.job_id) === id) {
      needs.push(String(dep.depends_on_job_id))
    }
    if (String(dep.depends_on_job_id) === id) {
      requiredBy.push(String(dep.job_id))
    }
  }

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
        <span className="text-primary">$</span>
        <Link to="/pipelines" className="hover:text-primary transition-colors">pipelines</Link>
        <span>/</span>
        {pipelineId ? (
          <Link to={`/pipelines/${pipelineId}`} className="hover:text-primary transition-colors">
            {pipelineName ?? pipelineId.slice(0, 8)}
          </Link>
        ) : (
          <span>...</span>
        )}
        <span>/</span>
        <span className="text-text-secondary">{jobData ? String(jobData.name) : id?.slice(0, 8)}</span>
      </div>

      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl sm:text-3xl font-black tracking-tight">
          {jobData ? String(jobData.name) : 'Loading...'}
        </h1>
        {jobData?.position != null && (
          <p className="text-sm text-text-secondary mt-1 font-mono">
            position {String(jobData.position)} in pipeline
          </p>
        )}
      </div>

      {/* Steps */}
      <div className="mb-8">
        <h2 className="text-lg font-bold mb-4">Steps</h2>
        {steps.length === 0 ? (
          <EmptyState icon="~" title="No steps" description="This job has no step definitions." />
        ) : (
          <div className="border border-dashed border-black/25 divide-y divide-dashed divide-black/10">
            {steps.map((step: any, i: number) => (
              <div key={String(step.id ?? i)} className="px-4 py-3 flex items-start justify-between gap-4">
                <div className="flex items-start gap-3 min-w-0">
                  <span className="text-xs font-mono text-text-muted pt-0.5 w-5 text-right shrink-0">
                    {step.position != null ? String(step.position) : i + 1}
                  </span>
                  <div className="min-w-0">
                    <div className="font-bold text-sm">{String(step.name)}</div>
                    {step.command && (
                      <div className="mt-1 px-2 py-1 bg-code-bg text-code-text text-xs font-mono border border-code-border">
                        &gt; {String(step.command)}
                      </div>
                    )}
                  </div>
                </div>
                {step.timeout != null && (
                  <span className="text-xs font-mono text-text-muted shrink-0 pt-0.5">
                    timeout: {String(step.timeout)}s
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Dependencies */}
      {(needs.length > 0 || requiredBy.length > 0) && (
        <div className="mb-8">
          <h2 className="text-lg font-bold mb-4">Dependencies</h2>
          <div className="border border-dashed border-black/25 px-4 py-3 space-y-2">
            {needs.length > 0 && (
              <div className="text-sm font-mono">
                <span className="text-text-muted">needs: </span>
                {needs.map((depId, i) => (
                  <span key={depId}>
                    {i > 0 && <span className="text-text-muted">, </span>}
                    <Link to={`/jobs/${depId}`} className="text-primary hover:underline">
                      {jobNameById[depId] ?? depId.slice(0, 8)}
                    </Link>
                  </span>
                ))}
              </div>
            )}
            {requiredBy.length > 0 && (
              <div className="text-sm font-mono">
                <span className="text-text-muted">required by: </span>
                {requiredBy.map((depId, i) => (
                  <span key={depId}>
                    {i > 0 && <span className="text-text-muted">, </span>}
                    <Link to={`/jobs/${depId}`} className="text-primary hover:underline">
                      {jobNameById[depId] ?? depId.slice(0, 8)}
                    </Link>
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Recent Runs */}
      <div>
        <h2 className="text-lg font-bold mb-4">Recent Runs</h2>
        {jobRuns.length === 0 ? (
          <EmptyState icon="~" title="No runs" description="This job has not been executed yet." />
        ) : (
          <div className="border border-dashed border-black/25 divide-y divide-dashed divide-black/10">
            {jobRuns.map((jr: any) => (
              <Link
                key={String(jr.id)}
                to={`/runs/${jr.run_id}`}
                className="px-4 py-3 flex items-center justify-between hover:bg-bg-secondary transition-colors block"
              >
                <div className="flex items-center gap-3">
                  <StatusBadge status={String(jr.status)} />
                  {jr.started_at && jr.finished_at && (
                    <span className="text-sm font-mono text-text-secondary">
                      {formatDuration(String(jr.started_at), String(jr.finished_at))}
                    </span>
                  )}
                </div>
                {jr.started_at && (
                  <span className="text-xs font-mono text-text-muted">
                    {formatDate(String(jr.started_at))}
                  </span>
                )}
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
