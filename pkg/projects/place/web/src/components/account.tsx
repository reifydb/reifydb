// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type FormEvent, useState } from 'react'
import { type GuestSession, signedInName } from '@/session'

export function Account({ guest }: { guest: GuestSession }) {
  const [name, setName] = useState<string | null>(() => signedInName())
  const [open, setOpen] = useState(false)
  const [identifier, setIdentifier] = useState('')
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  if (name) {
    return (
      <div className="panel absolute left-4 top-4 z-10 px-3 py-2 text-sm" title="Your pixels are credited to this name">
        Painting as <span className="font-bold">{name}</span>
      </div>
    )
  }

  if (!open) {
    return (
      <button type="button" className="game-button absolute left-4 top-4 z-10" onClick={() => setOpen(true)}>
        Sign up
      </button>
    )
  }

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await guest.signUp(identifier.trim(), password)
      setName(identifier.trim())
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure))
    } finally {
      setBusy(false)
    }
  }

  return (
    <form
      className="panel absolute left-4 top-4 z-10 flex w-64 flex-col gap-2 p-3 text-sm"
      aria-label="Sign up"
      onSubmit={submit}
      onPointerDown={(event) => event.stopPropagation()}
    >
      <p className="text-text-muted">Keep your pixels under your name. Everything you painted so far comes with you.</p>
      <input
        className="rounded border border-border-default px-2 py-1"
        placeholder="Name"
        autoComplete="username"
        value={identifier}
        onChange={(event) => setIdentifier(event.target.value)}
      />
      <input
        className="rounded border border-border-default px-2 py-1"
        type="password"
        placeholder="Password"
        autoComplete="new-password"
        value={password}
        onChange={(event) => setPassword(event.target.value)}
      />
      {error && <p className="text-status-error">Sign up failed: {error}</p>}
      <div className="flex justify-end gap-2">
        <button type="button" className="game-button game-button--secondary" onClick={() => setOpen(false)}>
          Cancel
        </button>
        <button type="submit" className="game-button" disabled={busy || identifier.trim() === '' || password === ''}>
          Sign up
        </button>
      </div>
    </form>
  )
}
