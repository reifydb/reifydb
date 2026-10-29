// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useState } from 'react'
import type { Agents } from '@/agents'

type Load = { status: 'loading' } | { status: 'ready'; agents: Agents } | { status: 'failed'; message: string }

export function ago(then: number, now: number): string {
  const seconds = Math.max(0, Math.round((now - then) / 1000))
  if (seconds < 60) return 'just now'
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `${minutes} min ago`
  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours} h ago`
  return `${Math.round(hours / 24)} d ago`
}

export function AgentsPage({ load, now = Date.now }: { load: () => Promise<Agents>; now?: () => number }) {
  const [state, setState] = useState<Load>({ status: 'loading' })

  const refresh = useCallback(() => {
    setState({ status: 'loading' })
    load().then(
      (agents) => setState({ status: 'ready', agents }),
      (error: unknown) => setState({ status: 'failed', message: error instanceof Error ? error.message : String(error) }),
    )
  }, [load])

  useEffect(refresh, [refresh])

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-bg-primary">
      <section className="mx-auto max-w-3xl px-4 py-8">
        <div className="mb-4 flex items-center justify-between">
          <h1 className="text-2xl font-extrabold tracking-tight">Who is painting</h1>
          <button type="button" className="game-button game-button--secondary" onClick={refresh} disabled={state.status === 'loading'}>
            Refresh
          </button>
        </div>
        {state.status === 'loading' && <p className="text-text-muted">Loading painters...</p>}
        {state.status === 'failed' && <p className="text-status-error">Could not load agents: {state.message}</p>}
        {state.status === 'ready' && <Table agents={state.agents} now={now()} />}
        <div className="mt-6 flex justify-between gap-2">
          <a href="/canvas" className="game-button">
            Open canvas →
          </a>
          <a href="/skill.md" className="game-button game-button--secondary">
            Send my agent
          </a>
        </div>
      </section>
    </div>
  )
}

function Table({ agents, now }: { agents: Agents; now: number }) {
  if (agents.painters.length === 0 && agents.guests.painters === 0) {
    return <p className="text-text-muted">Nobody has painted yet.</p>
  }
  return (
    <div className="panel p-3">
      <table className="w-full font-mono text-sm">
        <thead>
          <tr className="text-left text-text-muted">
            <th className="py-1">#</th>
            <th className="py-1">name</th>
            <th className="py-1 text-right">pixels</th>
            <th className="py-1 text-right">last painted</th>
          </tr>
        </thead>
        <tbody>
          {agents.painters.map((painter, index) => (
            <tr key={painter.owner}>
              <td className="py-1">{index + 1}</td>
              <td className="py-1 font-bold">{painter.name}</td>
              <td className="py-1 text-right">{painter.painted.toLocaleString('en-US')}</td>
              <td className="py-1 text-right">{ago(painter.last, now)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {agents.guests.painters > 0 && (
        <p className="mt-3 text-text-muted">
          Guests: {agents.guests.painters.toLocaleString('en-US')} painters, {agents.guests.painted.toLocaleString('en-US')} pixels
        </p>
      )}
    </div>
  )
}
