// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'
import { clearStoredSession } from '@reifydb/auth'
import { Client, type WsClient } from '@reifydb/client'
import { Loading } from '@reifydb/ui'
import { wsUrl } from './config'
import { loadAgents } from './agents'
import { AgentsPage } from './components/agents-page'
import { BrandBar } from './components/brand-bar'
import { Landing } from './components/landing'
import { Place } from './components/place'
import { type GuestSession, SESSION_NAMESPACE, guestSession, openConnection } from './session'

type Connection =
  | { status: 'connecting' }
  | { status: 'ready'; client: WsClient; guest: GuestSession }
  | { status: 'failed'; message: string }

function App() {
  const canvas = window.location.pathname.startsWith('/canvas')
  const agents = window.location.pathname.startsWith('/agents')
  const [connection, setConnection] = useState<Connection>({ status: 'connecting' })
  const [generation, setGeneration] = useState(0)

  useEffect(() => {
    let cancelled = false
    let opened: WsClient | null = null
    const restart = () => {
      if (cancelled) return
      clearStoredSession(SESSION_NAMESPACE)
      setConnection({ status: 'connecting' })
      setGeneration((current) => current + 1)
    }
    openConnection((token) =>
      Client.connectWs(wsUrl(), {
        token,
        format: 'rbcf',
        maxReconnectAttempts: Number.MAX_SAFE_INTEGER,
        reconnectDelayMs: 1000,
        onAuthFailed: restart,
      }),
    )
      .then(({ client, signedIn }) => {
        if (cancelled) {
          void client.disconnect()
          return
        }
        opened = client
        setConnection({ status: 'ready', client, guest: guestSession(client, signedIn) })
      })
      .catch((error: unknown) => {
        if (!cancelled) setConnection({ status: 'failed', message: error instanceof Error ? error.message : String(error) })
      })
    return () => {
      cancelled = true
      void opened?.disconnect()
    }
  }, [generation])

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-bg-primary">
      {canvas && <BrandBar />}
      {agents && <BrandBar tag="AGENTS" />}
      <main className="relative flex min-h-0 flex-1 flex-col">
        {connection.status === 'connecting' && <Loading />}
        {connection.status === 'failed' && (
          <p className="font-mono text-sm text-status-error">Could not connect to ReifyDB: {connection.message}</p>
        )}
        {connection.status === 'ready' &&
          (canvas ? (
            <Place client={connection.client} guest={connection.guest} />
          ) : agents ? (
            <AgentsPage load={() => loadAgents(connection.client)} />
          ) : (
            <Landing client={connection.client} />
          ))}
      </main>
    </div>
  )
}

export default App
