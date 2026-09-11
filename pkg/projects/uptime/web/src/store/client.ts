// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  Client,
  ReifyError,
  Store,
  type StoreClient,
  type StoreOptions,
  type SubscriptionCallbacks,
  type WsClient,
} from '@reifydb/react'
import { UPTIME_CONFIG } from '@/config'

// A page mounts its subscriptions in one React pass, so batching turns a dashboard's five round
// trips into one. The tests build their store from this same object so the two cannot drift.
export const STORE_OPTIONS: StoreOptions = {
  batch: true,
  onBackgroundError: (err) => console.error('uptime store background error', err),
}

const AUTH_ERROR_CODES = new Set(['AUTH_FAILED', 'AUTH_REQUIRED'])

const CONNECTION_FAILURES = ['WebSocket connection failed', 'WebSocket connection timeout']

const RECONNECT_DELAY_MS = 1000

const MAX_RECONNECT_DELAY_MS = 30_000

let client: WsClient | null = null
let store: Store | null = null
let currentToken: string | null = null
let generation = 0

export function isAuthError(err: unknown): boolean {
  return err instanceof ReifyError && AUTH_ERROR_CODES.has(err.code)
}

function watchAuth(c: WsClient, onAuthError: () => void): StoreClient {
  const watch = <T>(pending: Promise<T>): Promise<T> =>
    pending.catch((err: unknown) => {
      if (isAuthError(err)) onAuthError()
      throw err
    })
  const watchErrors = <T>(callbacks: SubscriptionCallbacks<T>): SubscriptionCallbacks<T> => ({
    ...callbacks,
    onError: (err) => {
      if (isAuthError(err)) onAuthError()
      callbacks.onError?.(err)
    },
  })
  return {
    query: (rql, params, shapes) => watch(c.query(rql, params, shapes)),
    command: (rql, params, shapes) => watch(c.command(rql, params, shapes)),
    admin: (rql, params, shapes) => watch(c.admin(rql, params, shapes)),
    subscribe: (rql, params, shape, callbacks, config) =>
      watch(c.subscribe(rql, params, shape, watchErrors(callbacks), config)),
    unsubscribe: (subscriptionId) => watch(c.unsubscribe(subscriptionId)),
    batchSubscribe: (members) =>
      watch(
        c.batchSubscribe(members.map((member) => ({ ...member, callbacks: watchErrors(member.callbacks) }))),
      ),
  }
}

function isConnectionFailure(err: unknown): boolean {
  return err instanceof Error && CONNECTION_FAILURES.some((failure) => err.message.startsWith(failure))
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function openSocket(token: string, gen: number): Promise<WsClient | undefined> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await Client.connectWs(UPTIME_CONFIG.wsUrl(), {
        token,
        format: 'rbcf',
        maxReconnectAttempts: Number.MAX_SAFE_INTEGER,
        reconnectDelayMs: RECONNECT_DELAY_MS,
      })
    } catch (err) {
      if (!isConnectionFailure(err)) throw err
    }
    await sleep(Math.min(RECONNECT_DELAY_MS * 2 ** attempt, MAX_RECONNECT_DELAY_MS))
    if (gen !== generation) return undefined
  }
}

export async function connect(token: string, onAuthError: (store: Store) => void): Promise<Store | undefined> {
  if (client != null && store != null && currentToken === token) return store
  const gen = ++generation
  await teardown()
  if (gen !== generation) return undefined
  currentToken = token
  const c = await openSocket(token, gen)
  if (c == null) return undefined
  if (gen !== generation) {
    void c.disconnect()
    return undefined
  }
  client = c
  let authFailed = false
  const connected: Store = new Store(
    watchAuth(c, () => {
      if (gen !== generation || authFailed) return
      authFailed = true
      onAuthError(connected)
    }),
    STORE_OPTIONS,
  )
  store = connected
  return connected
}

async function teardown(): Promise<void> {
  const c = client
  client = null
  store = null
  currentToken = null
  if (c != null) {
    try {
      await c.disconnect()
    } catch (err) {
      console.error('uptime realtime disconnect failed', err)
    }
  }
}

export async function disconnect(): Promise<void> {
  generation++
  await teardown()
}
