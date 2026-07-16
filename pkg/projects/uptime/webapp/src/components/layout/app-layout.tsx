// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Outlet } from '@tanstack/react-router'
import { Navbar } from './navbar.tsx'

export function AppLayout() {
  return (
    <div className="min-h-screen bg-bg-primary">
      <Navbar />
      <main className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
        <Outlet />
      </main>
    </div>
  )
}
