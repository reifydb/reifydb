import { useParams, Link } from 'react-router-dom'
import { useQueryExecutor, useCommandExecutor } from '@reifydb/react'
import { StatusBadge, Button, EmptyState } from '@/components/ui'
import { useEffect } from 'react'
import { useLiveData } from '@/hooks/use-live-data'

export function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>()
  const { results: pipelineResults, query: queryPipeline } = useQueryExecutor<any>()
  const { data: jobs } = useLiveData(
    `FROM forge::jobs | FILTER pipeline_id == uuid::v4("${id}") | SORT {position:ASC}`
  )
  const { data: deps } = useLiveData(
    `FROM forge::job_dependencies`
  )
  const { data: runs } = useLiveData(
    `FROM forge::runs | FILTER pipeline_id == uuid::v4("${id}") | SORT {started_at:DESC} | TAKE 10`
  )

  const { command, isExecuting } = useCommandExecutor()

  useEffect(() => {
    if (id) queryPipeline(`FROM forge::pipelines | FILTER id == uuid::v4("${id}")`)
  }, [id, queryPipeline])

  const triggerRun = () => {
    command(`CALL forge::run_pipeline(uuid::v4("${id}"))`)
  }

  const pipelineData = pipelineResults?.[0]?.rows?.[0] as any

  // Build dependency graph
  const jobIds = new Set(jobs.map((j: any) => String(j.id)))
  const depsMap = new Map<string, string[]>()
  for (const dep of deps) {
    const jobId = String(dep.job_id)
    if (!jobIds.has(jobId)) continue
    if (!depsMap.has(jobId)) depsMap.set(jobId, [])
    depsMap.get(jobId)!.push(String(dep.depends_on_job_id))
  }

  // Job name lookup
  const jobNameById: Record<string, string> = {}
  for (const job of jobs) jobNameById[String(job.id)] = String(job.name)

  // Topological sort into layers
  const assigned = new Set<string>()
  const layers: any[][] = []
  while (assigned.size < jobs.length) {
    const layer: any[] = []
    for (const job of jobs) {
      const jobId = String(job.id)
      if (assigned.has(jobId)) continue
      const depIds = depsMap.get(jobId) ?? []
      if (depIds.every((d: string) => assigned.has(d))) layer.push(job)
    }
    if (layer.length === 0) break
    for (const job of layer) assigned.add(String(job.id))
    layers.push(layer)
  }
  // Add unassigned jobs (circular deps) to last layer
  for (const job of jobs) {
    if (!assigned.has(String(job.id))) {
      if (layers.length === 0) layers.push([])
      layers[layers.length - 1].push(job)
    }
  }

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
        <span className="text-primary">$</span>
        <Link to="/pipelines" className="hover:text-primary transition-colors">pipelines</Link>
        <span>/</span>
        <span className="text-text-secondary">{pipelineData?.name ? String(pipelineData.name) : id?.slice(0, 8)}</span>
      </div>

      {/* Header */}
      <div className="flex items-start justify-between mb-8">
        <div>
          <h1 className="text-2xl sm:text-3xl font-black tracking-tight">
            {pipelineData?.name ? String(pipelineData.name) : 'Loading...'}
          </h1>
          {pipelineData?.description && (
            <p className="text-sm text-text-secondary mt-1">{String(pipelineData.description)}</p>
          )}
        </div>
        <Button onClick={triggerRun} disabled={isExecuting} size="sm">
          {isExecuting ? 'Triggering...' : 'Run Pipeline'}
        </Button>
      </div>

      {/* Jobs */}
      <div className="mb-8">
        <h2 className="text-lg font-bold mb-4">Jobs</h2>
        {jobs.length === 0 ? (
          <EmptyState icon="~" title="No jobs" description="Add jobs to this pipeline via the ReifyDB CLI." />
        ) : (
          <div className="flex items-center gap-0 overflow-x-auto py-2">
            {layers.map((layer, layerIdx) => (
              <div key={layerIdx} className="flex items-center">
                {layerIdx > 0 && (
                  <div className="px-3 text-text-muted text-lg font-mono select-none">→</div>
                )}
                <div className="flex flex-col gap-3">
                  {layer.map((job: any) => {
                    const depIds = depsMap.get(String(job.id)) ?? []
                    const depNames = depIds.map((d: string) => jobNameById[d]).filter(Boolean)
                    return (
                      <Link key={String(job.id)} to={`/jobs/${job.id}`} className="border border-dashed border-black/25 px-4 py-3 min-w-[120px] cursor-pointer hover:border-primary transition-colors block">
                        <div className="font-bold text-sm">{String(job.name)}</div>
                        {depNames.length > 0 && (
                          <div className="text-xs font-mono text-text-muted mt-1">
                            needs: {depNames.join(', ')}
                          </div>
                        )}
                      </Link>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Runs */}
      <div>
        <h2 className="text-lg font-bold mb-4">Runs</h2>
        {runs.length === 0 ? (
          <EmptyState icon="~" title="No runs" description="Trigger a run to see execution history." />
        ) : (
          <div className="border border-dashed border-black/25 overflow-hidden">
            <table className="w-full text-sm font-mono">
              <thead>
                <tr className="border-b border-dashed border-black/25 bg-bg-secondary">
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Run ID</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Status</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Triggered By</th>
                  <th className="px-4 py-2.5 text-left text-xs font-bold uppercase tracking-wider text-text-muted">Started</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run: any) => (
                  <tr key={String(run.id)} className="border-b border-dashed border-black/10 hover:bg-bg-secondary transition-colors">
                    <td className="px-4 py-3">
                      <Link to={`/runs/${run.id}`} className="text-primary hover:underline">
                        {String(run.id).slice(0, 8)}...
                      </Link>
                    </td>
                    <td className="px-4 py-3"><StatusBadge status={String(run.status)} /></td>
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
