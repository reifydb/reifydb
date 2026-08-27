// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const SIGNED_OUT_KEY = 'reifydb.uptime.signed-out'

export function markSignedOut(): void {
  try {
    sessionStorage.setItem(SIGNED_OUT_KEY, '1')
  } catch {
    void 0
  }
}

export function clearSignedOut(): void {
  try {
    sessionStorage.removeItem(SIGNED_OUT_KEY)
  } catch {
    void 0
  }
}

export function isSignedOut(): boolean {
  try {
    return sessionStorage.getItem(SIGNED_OUT_KEY) === '1'
  } catch {
    return false
  }
}
