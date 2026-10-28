// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, type FormEvent } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { useMe } from '@/hooks/use-me'
import { clearSignedOut } from '@/lib/session-flags'
import { Button, Card, CardContent, CardHeader, CardTitle, Input } from '@reifydb/ui'

export function RegisterPage() {
  const { signIn, status, error: authError } = useAuth()
  const { data: me } = useMe()
  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const alreadyRegistered = status === 'authenticated' && me != null && !me.guest

  useEffect(() => {
    if (alreadyRegistered) {
      void navigate({ to: '/monitors' })
    }
  }, [alreadyRegistered, navigate])

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setFormError(null)
    const normalized = email.trim().toLowerCase()
    if (password !== confirm) {
      setFormError('Passwords do not match')
      return
    }
    setSubmitting(true)
    try {
      await signIn({ identifier: normalized, password })
      clearSignedOut()
    } finally {
      setSubmitting(false)
    }
  }

  const shownError =
    formError ??
    (authError === 'invalid credentials'
      ? 'This email already has an account, sign in instead'
      : authError)

  const busy = submitting || status === 'signing' || status === 'verifying'

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Create account</CardTitle>
      </CardHeader>
      <CardContent>
        {me?.guest === true && (
          <p className="mb-4 text-sm text-text-secondary">
            Your monitors stay exactly where they are - this account takes them over.
          </p>
        )}
        <form onSubmit={onSubmit} className="space-y-4">
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
            autoComplete="new-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
          <Input
            id="confirm"
            label="Confirm password"
            type="password"
            autoComplete="new-password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            required
          />
          {shownError != null && (
            <p className="text-sm text-status-error">{shownError}</p>
          )}
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? 'Creating account...' : 'Create account'}
          </Button>
        </form>
        <p className="mt-4 text-sm text-text-muted">
          Already have an account?{' '}
          <Link to="/login" className="text-primary-dark hover:underline">
            Sign in
          </Link>
        </p>
      </CardContent>
    </Card>
  )
}
