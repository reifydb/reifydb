// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { storageKeyFor, type AuthSession } from '@reifydb/auth'
import { UPTIME_CONFIG } from '@/config'

const SIGNED_OUT_KEY = 'reifydb.uptime.signed-out'

export function markSignedOut(): void {
  try {
    localStorage.setItem(SIGNED_OUT_KEY, '1')
  } catch {
    void 0
  }
}

export function clearSignedOut(): void {
  try {
    localStorage.removeItem(SIGNED_OUT_KEY)
  } catch {
    void 0
  }
}

export function isSignedOut(): boolean {
  try {
    return localStorage.getItem(SIGNED_OUT_KEY) === '1'
  } catch {
    return false
  }
}

function storedSession(): Partial<AuthSession> | null {
  try {
    const raw = localStorage.getItem(storageKeyFor(UPTIME_CONFIG.storageNamespace))
    return raw == null ? null : (JSON.parse(raw) as Partial<AuthSession>)
  } catch {
    return null
  }
}

export function markExpiredUserSignedOut(): void {
  const stored = storedSession()
  if (stored?.method !== 'password' || typeof stored.expiresAt !== 'number') return
  if (stored.expiresAt <= Date.now() / 1000) markSignedOut()
}
