// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type ReactNode } from 'react'
import { Navigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { Loading } from '@reifydb/ui'

export function RequireAuth({ children }: { children: ReactNode }) {
  const { status } = useAuth()

  if (status === 'authenticated') return <>{children}</>

  if (status === 'verifying' || status === 'signing') {
    return (
      <div className="min-h-screen bg-bg-primary flex items-center justify-center">
        <Loading />
      </div>
    )
  }

  return <Navigate to="/login" />
}
