// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { PlaceClient } from './client'
import { tilesParam } from './pack'
import type { Pending } from './pending'

export type CommitResult = { ok: true; committed: number } | { ok: false; message: string }

export interface Retry {
  retries: number
  baseDelayMs: number
  sleep: (ms: number) => Promise<void>
  random: () => number
}

export const DEFAULT_RETRY: Retry = {
  retries: 5,
  baseDelayMs: 50,
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  random: Math.random,
}

export async function commitPending(
  client: PlaceClient,
  pending: Pending,
  retry: Retry = DEFAULT_RETRY,
): Promise<CommitResult> {
  const pixels = pending.entries()
  if (pixels.length === 0) return { ok: true, committed: 0 }
  for (let attempt = 0; ; attempt++) {
    try {
      await client.command('CALL place::commit($tiles)', { tiles: tilesParam(pixels) }, [])
      break
    } catch (error) {
      if (!isConflict(error) || attempt >= retry.retries) {
        return { ok: false, message: error instanceof Error ? error.message : String(error) }
      }
      const backoff = retry.baseDelayMs * 2 ** attempt
      await retry.sleep(backoff + retry.random() * backoff)
    }
  }
  pending.settle(pixels)
  return { ok: true, committed: pixels.length }
}

function isConflict(error: unknown): boolean {
  if (error !== null && typeof error === 'object' && 'code' in error && error.code === 'TXN_001') return true
  return error instanceof Error && error.message.includes('TXN_001')
}
