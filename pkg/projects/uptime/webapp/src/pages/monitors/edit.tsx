// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNavigate, useParams } from '@tanstack/react-router'
import { useMonitor, useUpdateMonitor } from '@/hooks/use-monitors'
import type { MonitorInput } from '@/lib/types'
import { MonitorForm } from './monitor-form.tsx'

export function MonitorEditPage() {
  const { monitorId } = useParams({ strict: false }) as { monitorId: string }
  const navigate = useNavigate()
  const { data: monitor, isLoading, error } = useMonitor(monitorId)
  const update = useUpdateMonitor(monitorId)

  function onSubmit(input: MonitorInput) {
    update.mutate(input, {
      onSuccess: () => {
        void navigate({ to: '/monitors/$monitorId', params: { monitorId } })
      },
    })
  }

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading...</p>
  if (error != null || monitor == null) {
    return <p className="text-sm text-destructive">Monitor not found</p>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Edit {monitor.name}</h1>
      <MonitorForm
        monitor={monitor}
        submitting={update.isPending}
        submitError={update.error?.message ?? null}
        onSubmit={onSubmit}
      />
    </div>
  )
}
