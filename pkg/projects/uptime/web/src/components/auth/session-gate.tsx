// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, type ReactNode } from 'react'
import { Navigate } from '@tanstack/react-router'
import { readStoredSession, useAuth, type AuthSession } from '@reifydb/auth'
import { Button, Loading } from '@reifydb/ui'
import { apiFetch } from '@/lib/api'
import { UPTIME_CONFIG } from '@/config'
import { isSignedOut } from '@/lib/session-flags'
import type { GuestSession } from '@/lib/types'

let inFlightGuestSession: Promise<AuthSession> | null = null

// Every tab shares one storage slot, and a tab whose session is replaced tears
// its own down. Minting unconditionally would make two tabs steal the session
// from each other forever, so an existing session always wins over a new guest.
function existingOrNewGuestSession(): Promise<AuthSession> {
  const stored = readStoredSession(UPTIME_CONFIG.storageNamespace)
  if (stored != null) return Promise.resolve(stored)
  return mintGuestSession()
}

function mintGuestSession(): Promise<AuthSession> {
  inFlightGuestSession ??= apiFetch<GuestSession>('/auth/guest', { method: 'POST' })
    .then((session) => ({
      token: session.token,
      identity: session.identity,
      wallet_address: session.identity,
      expires_at: session.expires_at,
      method: 'token' as const,
    }))
    .finally(() => {
      inFlightGuestSession = null
    })
  return inFlightGuestSession
}

function Centered({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-bg-primary flex items-center justify-center">
      {children}
    </div>
  )
}

export function SessionGate({ children }: { children: ReactNode }) {
  const { status, adoptSession, signOut } = useAuth()
  const [failed, setFailed] = useState(false)
  const [attempt, setAttempt] = useState(0)

  useEffect(() => {
    if (status !== 'disconnected' || isSignedOut()) return
    let cancelled = false
    setFailed(false)
    existingOrNewGuestSession()
      .then((session) => {
        if (!cancelled) adoptSession(session)
      })
      .catch(() => {
        if (!cancelled) setFailed(true)
      })
    return () => {
      cancelled = true
    }
  }, [status, adoptSession, attempt])

  if (status === 'authenticated') return <>{children}</>

  if (status === 'disconnected' && isSignedOut()) return <Navigate to="/login" />

  if (failed || status === 'error') {
    const retry = () => {
      setFailed(false)
      setAttempt((n) => n + 1)
      void signOut()
    }
    return (
      <Centered>
        <div className="text-center">
          <p className="mb-4 text-sm text-text-secondary">
            Could not start a session.
          </p>
          <Button onClick={retry}>Try again</Button>
        </div>
      </Centered>
    )
  }

  return (
    <Centered>
      <Loading />
    </Centered>
  )
}
