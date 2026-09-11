// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNavigate, useParams } from '@tanstack/react-router'
import { useUpdateMonitor } from '@/hooks/use-monitors'
import { useLiveMonitor, useMonitorRegions } from '@/store/realtime'
import { errorMessage, rethrowUnrecorded } from '@/lib/errors'
import type { MonitorInput } from '@/lib/types'
import { Loading } from '@reifydb/ui'
import { MonitorForm } from './monitor-form.tsx'

export function MonitorEditPage() {
  const { monitorId } = useParams({ strict: false }) as { monitorId: string }
  const navigate = useNavigate()
  const { monitor, ready, error: monitorError } = useLiveMonitor(monitorId)
  const monitorRegions = useMonitorRegions(monitorId)
  const { update, isPending, error } = useUpdateMonitor(monitorId)

  function onSubmit(input: MonitorInput) {
    void update(input).then(
      () => navigate({ to: '/monitors/$monitorId', params: { monitorId } }),
      rethrowUnrecorded,
    )
  }

  const loadError = monitorError ?? monitorRegions.error
  if (loadError != null) {
    return <p className="text-sm text-status-error">Failed to load monitor: {errorMessage(loadError)}</p>
  }
  if (!ready || !monitorRegions.ready) return <Loading />
  if (monitor == null) {
    return <p className="text-sm text-status-error">Monitor not found</p>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl">Edit {monitor.name}</h1>
      <MonitorForm
        monitor={monitor}
        initialRegions={monitorRegions.data.map((mr) => mr.region_id)}
        submitting={isPending}
        submitError={error == null ? null : errorMessage(error)}
        onSubmit={onSubmit}
      />
    </div>
  )
}
