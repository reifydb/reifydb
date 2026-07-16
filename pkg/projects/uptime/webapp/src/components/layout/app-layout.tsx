// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Outlet } from '@tanstack/react-router'
import { Sidebar } from './sidebar.tsx'

export function AppLayout() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main className="flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
