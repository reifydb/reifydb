// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { PlaceClient } from './client'
import { type Pixel, tilesParam } from './pack'
import type { Pending } from './pending'
import type { GuestSession } from './session'

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
  guest?: Pick<GuestSession, 'ensure'>,
): Promise<CommitResult> {
  const pixels = pending.entries()
  const result = await commitPixels(client, pixels, retry, guest)
  if (result.ok && pixels.length > 0) pending.settle(pixels)
  return result
}

export async function commitPixels(
  client: PlaceClient,
  pixels: readonly Pixel[],
  retry: Retry = DEFAULT_RETRY,
  guest?: Pick<GuestSession, 'ensure'>,
): Promise<CommitResult> {
  if (pixels.length === 0) return { ok: true, committed: 0 }
  try {
    await guest?.ensure()
  } catch (error) {
    return { ok: false, message: error instanceof Error ? error.message : String(error) }
  }
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
  return { ok: true, committed: pixels.length }
}

function isConflict(error: unknown): boolean {
  if (error !== null && typeof error === 'object' && 'code' in error && error.code === 'TXN_001') return true
  return error instanceof Error && error.message.includes('TXN_001')
}
