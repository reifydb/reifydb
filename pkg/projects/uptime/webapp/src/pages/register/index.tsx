// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, type FormEvent } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import { useAuth } from '@reifydb/auth'
import { apiFetch, ApiError } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

export function RegisterPage() {
  const { signIn, status, error: authError } = useAuth()
  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (status === 'authenticated') {
      void navigate({ to: '/dashboard' })
    }
  }, [status, navigate])

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setFormError(null)
    const normalized = email.trim().toLowerCase()
    if (password.length < 8) {
      setFormError('Password must be at least 8 characters')
      return
    }
    if (password !== confirm) {
      setFormError('Passwords do not match')
      return
    }
    setSubmitting(true)
    try {
      await apiFetch<void>('/auth/register', {
        method: 'POST',
        body: { email: normalized, password },
      })
      await signIn({ identifier: normalized, password })
    } catch (err) {
      if (err instanceof ApiError) {
        setFormError(err.message)
      } else {
        setFormError('Registration failed')
      }
    } finally {
      setSubmitting(false)
    }
  }

  const busy = submitting || status === 'signing' || status === 'verifying'

  return (
    <Card>
      <CardHeader>
        <CardTitle>Create account</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={onSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="email">Email</Label>
            <Input
              id="email"
              type="email"
              autoComplete="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="confirm">Confirm password</Label>
            <Input
              id="confirm"
              type="password"
              autoComplete="new-password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              required
            />
          </div>
          {(formError != null || (authError != null && status === 'error')) && (
            <p className="text-sm text-destructive">{formError ?? authError}</p>
          )}
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? 'Creating account...' : 'Create account'}
          </Button>
        </form>
        <p className="mt-4 text-sm text-muted-foreground">
          Already have an account?{' '}
          <Link to="/login" className="text-primary hover:underline">
            Sign in
          </Link>
        </p>
      </CardContent>
    </Card>
  )
}
