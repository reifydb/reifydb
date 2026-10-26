// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'
import { Client, type WsClient } from '@reifydb/client'
import { Loading } from '@reifydb/ui'
import { wsUrl } from './config'
import { BrandBar } from './components/brand-bar'
import { Landing } from './components/landing'
import { Place } from './components/place'

type Connection =
  | { status: 'connecting' }
  | { status: 'ready'; client: WsClient }
  | { status: 'failed'; message: string }

function App() {
  const canvas = window.location.pathname.startsWith('/canvas')
  const [connection, setConnection] = useState<Connection>({ status: 'connecting' })

  useEffect(() => {
    let cancelled = false
    let opened: WsClient | null = null
    Client.connectWs(wsUrl(), {
      format: 'rbcf',
      maxReconnectAttempts: Number.MAX_SAFE_INTEGER,
      reconnectDelayMs: 1000,
    })
      .then((client) => {
        if (cancelled) {
          void client.disconnect()
          return
        }
        opened = client
        setConnection({ status: 'ready', client })
      })
      .catch((error: unknown) => {
        if (!cancelled) setConnection({ status: 'failed', message: error instanceof Error ? error.message : String(error) })
      })
    return () => {
      cancelled = true
      void opened?.disconnect()
    }
  }, [])

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-bg-primary">
      {canvas && <BrandBar tag="AGENT CANVAS" />}
      <main className="relative flex min-h-0 flex-1 flex-col">
        {connection.status === 'connecting' && <Loading />}
        {connection.status === 'failed' && (
          <p className="font-mono text-sm text-status-error">Could not connect to ReifyDB: {connection.message}</p>
        )}
        {connection.status === 'ready' && (canvas ? <Place client={connection.client} /> : <Landing client={connection.client} />)}
      </main>
    </div>
  )
}

export default App
