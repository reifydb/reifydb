// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Client, Store, type StoreOptions, type WsClient } from '@reifydb/react'
import { create } from 'zustand'
import { UPTIME_CONFIG } from '@/config'

export type ConnectionStatus = 'offline' | 'connecting' | 'live' | 'reconnecting'

interface ConnectionState {
  status: ConnectionStatus
}

const useConnection = create<ConnectionState>(() => ({ status: 'offline' }))

function setStatus(status: ConnectionStatus) {
  useConnection.setState({ status })
}

export function useConnectionStatus(): ConnectionStatus {
  return useConnection((s) => s.status)
}

// A page mounts its subscriptions in one React pass, so batching turns a dashboard's five round
// trips into one. The tests build their store from this same object so the two cannot drift.
export const STORE_OPTIONS: StoreOptions = {
  batch: true,
  onBackgroundError: (err) => console.error('uptime store background error', err),
}

let client: WsClient | null = null
let store: Store | null = null
let currentToken: string | null = null
let generation = 0

export async function connect(token: string): Promise<Store | undefined> {
  if (client != null && store != null && currentToken === token) return store
  const gen = ++generation
  await teardown()
  if (gen !== generation) return undefined
  currentToken = token
  setStatus('connecting')
  try {
    const c = await Client.connectWs(UPTIME_CONFIG.wsUrl(), {
      token,
      format: 'rbcf',
      maxReconnectAttempts: Number.MAX_SAFE_INTEGER,
      reconnectDelayMs: 1000,
      onDisconnect: () => {
        if (gen === generation) setStatus('reconnecting')
      },
      onReconnect: () => {
        if (gen === generation) setStatus('live')
      },
    })
    if (gen !== generation) {
      void c.disconnect()
      return undefined
    }
    client = c
    store = new Store(c, STORE_OPTIONS)
    setStatus('live')
    return store
  } catch (err) {
    console.error('uptime realtime connect failed', err)
    if (gen === generation) setStatus('offline')
    return undefined
  }
}

async function teardown(): Promise<void> {
  const c = client
  client = null
  store = null
  currentToken = null
  setStatus('offline')
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
