// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNavigate } from '@tanstack/react-router'
import { useCreateMonitor } from '@/hooks/use-monitors'
import type { MonitorInput } from '@/lib/types'
import { MonitorForm } from './monitor-form.tsx'

export function MonitorNewPage() {
  const navigate = useNavigate()
  const create = useCreateMonitor()

  function onSubmit(input: MonitorInput) {
    create.mutate(input, {
      onSuccess: (monitor) => {
        void navigate({ to: '/monitors/$monitorId', params: { monitorId: monitor.id } })
      },
    })
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl">New monitor</h1>
      <MonitorForm
        submitting={create.isPending}
        submitError={create.error?.message ?? null}
        onSubmit={onSubmit}
      />
    </div>
  )
}
