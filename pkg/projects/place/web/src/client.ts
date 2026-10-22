// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { BatchSubscribeItem, BatchSubscription } from '@reifydb/client'

export interface PlaceClient {
  query(rql: string, params: any, shapes: []): Promise<any>
  command(rql: string, params: any, shapes: []): Promise<any>
  batchSubscribe(items: BatchSubscribeItem[]): Promise<BatchSubscription>
  batchUnsubscribe(batchId: string): Promise<void>
}

export function numeric(value: unknown): number {
  if (value !== null && typeof value === 'object' && 'value' in value) return Number((value as { value: unknown }).value)
  return Number(value)
}
