// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'
import { Outlet } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { StoreProvider, type Store } from '@reifydb/react'
import { Loading } from '@reifydb/ui'
import { connect, disconnect } from '@/store/client'
import { useMe } from '@/hooks/use-me'
import { GuestBanner } from './guest-banner.tsx'
import { Navbar } from './navbar.tsx'

export function AppLayout() {
  const { session } = useAuth()
  const token = session?.token
  const { data: me } = useMe()
  const [store, setStore] = useState<Store | undefined>(undefined)

  useEffect(() => {
    let cancelled = false
    if (token) {
      void connect(token).then((connected) => {
        if (!cancelled) setStore(connected)
      })
    } else {
      void disconnect()
    }
    return () => {
      cancelled = true
      setStore(undefined)
      void disconnect()
    }
  }, [token])

  return (
    <div className="min-h-screen bg-bg-primary">
      <Navbar />
      {me?.guest === true && <GuestBanner />}
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
  )
}
