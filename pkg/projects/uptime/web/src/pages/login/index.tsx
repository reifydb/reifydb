// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, type FormEvent } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import { useQueryClient } from '@tanstack/react-query'
import { useAuth } from '@reifydb/auth'
import { useMe } from '@/hooks/use-me'
import { clearSignedOut } from '@/lib/session-flags'
import { Button, Card, CardContent, CardHeader, CardTitle, Input } from '@reifydb/ui'

export function LoginPage() {
  const { signIn, status, error } = useAuth()
  const { data: me } = useMe()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')

  const alreadyRegistered = status === 'authenticated' && me != null && !me.guest

  useEffect(() => {
    if (alreadyRegistered) {
      void navigate({ to: '/monitors' })
    }
  }, [alreadyRegistered, navigate])

  const busy = status === 'signing' || status === 'verifying'

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    if (email.trim().length === 0 || password.length === 0) return
    await signIn({ identifier: email.trim().toLowerCase(), password })
    clearSignedOut()
    await queryClient.invalidateQueries({ queryKey: ['me'] })
  }

  function continueAsGuest() {
    clearSignedOut()
    void navigate({ to: '/monitors' })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Sign in</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={(e) => void onSubmit(e)} className="space-y-4">
          <Input
            id="email"
            label="Email"
            type="email"
            autoComplete="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
          <Input
            id="password"
            label="Password"
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
          {error != null && status === 'error' && (
            <p className="text-sm text-status-error">{error}</p>
          )}
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? 'Signing in...' : 'Sign in'}
          </Button>
        </form>
        <p className="mt-4 text-sm text-text-muted">
          No account?{' '}
          <Link to="/register" className="text-primary-dark hover:underline">
            Register
          </Link>
          {' or '}
          <button
            type="button"
            onClick={continueAsGuest}
            className="text-primary-dark hover:underline"
          >
            continue as guest
          </button>
        </p>
      </CardContent>
    </Card>
  )
}
