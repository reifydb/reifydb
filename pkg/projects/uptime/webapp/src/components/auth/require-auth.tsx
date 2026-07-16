// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type ReactNode } from 'react'
import { Navigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'

export function RequireAuth({ children }: { children: ReactNode }) {
  const { status } = useAuth()

  if (status === 'authenticated') return <>{children}</>

  if (status === 'verifying' || status === 'signing') {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <p className="text-sm text-muted-foreground animate-pulse">Loading...</p>
      </div>
    )
  }

  return <Navigate to="/login" />
}
