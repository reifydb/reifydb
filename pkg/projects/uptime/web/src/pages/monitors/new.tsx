// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNavigate } from '@tanstack/react-router'
import { useCreateMonitor } from '@/hooks/use-monitors'
import type { MonitorInput } from '@/lib/types'
import { MonitorForm } from './monitor-form.tsx'

export function MonitorNewPage() {
  const navigate = useNavigate()
  const { create, isPending, error } = useCreateMonitor()

  function onSubmit(input: MonitorInput) {
    // the hook keeps the error for the form, so the rejection only needs to be settled here
    void create(input).then(
      (id) => navigate({ to: '/monitors/$monitorId', params: { monitorId: id } }),
      () => undefined,
    )
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl">New monitor</h1>
      <MonitorForm
        submitting={isPending}
        submitError={error?.message ?? null}
        onSubmit={onSubmit}
      />
    </div>
  )
}
