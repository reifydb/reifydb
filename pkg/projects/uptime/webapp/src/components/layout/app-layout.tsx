// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect } from 'react'
import { Outlet } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { startRealtime, stopRealtime } from '@/store/subscription-manager'
import { Navbar } from './navbar.tsx'

export function AppLayout() {
  const { session } = useAuth()
  const token = session?.token

  useEffect(() => {
    if (token) void startRealtime(token)
    else void stopRealtime()
    return () => {
      void stopRealtime()
    }
  }, [token])

  return (
    <div className="min-h-screen bg-bg-primary">
      <Navbar />
      <main className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
        <Outlet />
      </main>
    </div>
  )
}
