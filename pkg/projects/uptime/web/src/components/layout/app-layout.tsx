// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Outlet } from '@tanstack/react-router'
import { StoreProvider } from '@reifydb/react'
import { Loading } from '@reifydb/ui'
import { MeProvider } from '@/hooks/use-me'
import { useSessionStore } from '@/hooks/use-session-store'
import { GuestBanner } from './guest-banner.tsx'
import { Navbar } from './navbar.tsx'

export function AppLayout() {
  const store = useSessionStore()

  return (
    <MeProvider store={store}>
      <div className="min-h-screen bg-bg-primary">
        <Navbar />
        <GuestBanner />
        <main className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
          {store == null ? (
            <Loading />
          ) : (
            <StoreProvider store={store}>
              <Outlet />
            </StoreProvider>
          )}
        </main>
      </div>
    </MeProvider>
  )
}
