// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'
import { Client, type WsClient } from '@reifydb/client'
import { Loading } from '@reifydb/ui'
import { wsUrl } from './config'
import { Header } from './components/header'
import { Place } from './components/place'

type Connection =
  | { status: 'connecting' }
  | { status: 'ready'; client: WsClient }
  | { status: 'failed'; message: string }

function App() {
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
    <div className="flex min-h-screen flex-col bg-bg-primary">
      <Header status={connection.status} />
      <main className="mx-auto flex w-full max-w-[1600px] flex-1 flex-col px-4 py-6 sm:px-6">
        {connection.status === 'connecting' && <Loading />}
        {connection.status === 'failed' && (
          <p className="font-mono text-sm text-status-error">Could not connect to ReifyDB: {connection.message}</p>
        )}
        {connection.status === 'ready' && <Place client={connection.client} />}
      </main>
    </div>
  )
}

export default App
