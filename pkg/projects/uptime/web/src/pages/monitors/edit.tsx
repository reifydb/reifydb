// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNavigate, useParams } from '@tanstack/react-router'
import { useUpdateMonitor } from '@/hooks/use-monitors'
import { useLiveMonitor, useMonitorRegions } from '@/store/realtime'
import type { MonitorInput } from '@/lib/types'
import { Loading } from '@reifydb/ui'
import { MonitorForm } from './monitor-form.tsx'

export function MonitorEditPage() {
  const { monitorId } = useParams({ strict: false }) as { monitorId: string }
  const navigate = useNavigate()
  const { monitor, ready } = useLiveMonitor(monitorId)
  const monitorRegions = useMonitorRegions(monitorId)
  const update = useUpdateMonitor(monitorId)

  function onSubmit(input: MonitorInput) {
    update.mutate(input, {
      onSuccess: () => {
        void navigate({ to: '/monitors/$monitorId', params: { monitorId } })
      },
    })
  }

  if (!ready) return <Loading />
  if (monitor == null) {
    return <p className="text-sm text-status-error">Monitor not found</p>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl">Edit {monitor.name}</h1>
      <MonitorForm
        monitor={monitor}
        initialRegions={monitorRegions.map((mr) => mr.region_id)}
        submitting={update.isPending}
        submitError={update.error?.message ?? null}
        onSubmit={onSubmit}
      />
    </div>
  )
}
