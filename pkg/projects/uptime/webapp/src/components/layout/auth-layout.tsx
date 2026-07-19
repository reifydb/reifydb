// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Outlet } from '@tanstack/react-router'

export function AuthLayout() {
  return (
    <div className="min-h-screen bg-bg-primary flex items-center justify-center p-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center justify-center mb-8">
          <span className="font-mono text-xl font-bold text-text-primary">Uptime</span>
        </div>
        <Outlet />
      </div>
    </div>
  )
}
