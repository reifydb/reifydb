// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { Link } from 'react-router-dom'
import { EmptyState } from '@/components/ui'
import { useLiveData } from '@/hooks/use-live-data'

export function PipelinesPage() {
  const { data: pipelines } = useLiveData(
    'FROM forge::pipelines | SORT {created_at:DESC}'
  )

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
          <span className="text-primary">$</span> forge pipelines list
        </div>
        <h1 className="text-2xl sm:text-3xl font-black tracking-tight">Pipelines</h1>
      </div>

      {pipelines.length === 0 ? (
        <EmptyState
          icon="+"
          title="No pipelines"
          description="Pipelines define the stages and steps of your CI workflow. Insert pipeline data via the ReifyDB CLI."
        />
      ) : (
        <div className="grid gap-4">
          {pipelines.map((pipeline: any) => (
            <Link
              key={String(pipeline.id)}
              to={`/pipelines/${pipeline.id}`}
              className="border border-dashed border-black/25 p-5 hover:bg-bg-secondary transition-colors group"
            >
              <div className="flex items-start justify-between">
                <div>
                  <h3 className="font-bold text-text-primary group-hover:text-primary transition-colors">
                    {String(pipeline.name)}
                  </h3>
                  <p className="text-sm text-text-secondary mt-1">
                    {String(pipeline.description || 'No description')}
                  </p>
                </div>
                <span className="text-xs font-mono text-text-muted">
                  {String(pipeline.id).slice(0, 8)}...
                </span>
              </div>
              <div className="flex items-center gap-4 mt-3 text-xs font-mono text-text-muted">
                <span>created: {String(pipeline.created_at)}</span>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
