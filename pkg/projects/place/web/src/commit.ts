// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { PlaceClient } from './client'
import { pixelsParam } from './pack'
import type { Pending } from './pending'

export type CommitResult = { ok: true; committed: number } | { ok: false; message: string }

export async function commitPending(client: PlaceClient, pending: Pending): Promise<CommitResult> {
  const pixels = pending.entries()
  if (pixels.length === 0) return { ok: true, committed: 0 }
  try {
    await client.command('CALL place::commit($pixels)', { pixels: pixelsParam(pixels) }, [])
  } catch (error) {
    return { ok: false, message: error instanceof Error ? error.message : String(error) }
  }
  pending.settle(pixels)
  return { ok: true, committed: pixels.length }
}
