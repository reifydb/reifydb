// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Outlet } from '@tanstack/react-router'

export function AuthLayout() {
  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center justify-center gap-3 mb-8">
          <img
            src="/assets/favicon-128x128.png"
            alt="ReifyDB Logo"
            className="h-10 w-10 object-contain"
          />
          <span className="font-semibold text-xl">ReifyDB Uptime</span>
        </div>
        <Outlet />
      </div>
    </div>
  )
}
