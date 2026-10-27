// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { SubscribeItem } from '@reifydb/client'

export interface PlaceClient {
  query(rql: string, params: any, shapes: []): Promise<any>
  command(rql: string, params: any, shapes: []): Promise<any>
  subscribe(subscriptions: SubscribeItem[]): Promise<string[]>
  unsubscribe(subscriptionIds: string[]): Promise<void>
}

export function numeric(value: unknown): number {
  if (value !== null && typeof value === 'object' && 'value' in value) return Number((value as { value: unknown }).value)
  return Number(value)
}

export function bytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value
  if (value !== null && typeof value === 'object' && 'value' in value && value.value instanceof Uint8Array) return value.value
  throw new Error(`expected a blob, got ${String(value)}`)
}

export function version(value: unknown): bigint {
  const inner = value !== null && typeof value === 'object' && 'value' in value ? value.value : value
  if (typeof inner === 'bigint' || typeof inner === 'number' || typeof inner === 'string') return BigInt(inner)
  throw new Error(`expected a commit version, got ${String(value)}`)
}
