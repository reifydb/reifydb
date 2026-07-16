// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type FormEvent } from 'react'
import type { Monitor, MonitorInput, MonitorKind } from '@/lib/types'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'

const TARGET_LABEL: Record<MonitorKind, { label: string; placeholder: string }> = {
  http: { label: 'URL', placeholder: 'https://example.com/health' },
  tcp: { label: 'Host and port', placeholder: 'example.com:5432' },
  ping: { label: 'Host', placeholder: 'example.com' },
  dns: { label: 'Hostname', placeholder: 'example.com' },
}

interface FormState {
  name: string
  kind: MonitorKind
  target: string
  interval_s: string
  timeout_s: string
  failure_threshold: string
  enabled: boolean
  http_method: string
  expected_status: string
  keyword: string
  expected_ip: string
}

function initial_state(monitor?: Monitor): FormState {
  return {
    name: monitor?.name ?? '',
    kind: monitor?.kind ?? 'http',
    target: monitor?.target ?? '',
    interval_s: String(Math.round((monitor?.interval_ms ?? 60_000) / 1000)),
    timeout_s: String(Math.round((monitor?.timeout_ms ?? 10_000) / 1000)),
    failure_threshold: String(monitor?.failure_threshold ?? 1),
    enabled: monitor?.enabled ?? true,
    http_method: monitor?.http_method ?? 'GET',
    expected_status: monitor?.expected_status != null ? String(monitor.expected_status) : '',
    keyword: monitor?.keyword ?? '',
    expected_ip: monitor?.expected_ip ?? '',
  }
}

function validate(s: FormState): string | null {
  if (s.name.trim().length === 0) return 'Name is required'
  const target = s.target.trim()
  if (target.length === 0) return 'Target is required'
  if (s.kind === 'http' && !/^https?:\/\//.test(target)) {
    return 'URL must start with http:// or https://'
  }
  if (s.kind === 'tcp' && !/^.+:\d+$/.test(target)) {
    return 'TCP target must be host:port'
  }
  const interval = Number(s.interval_s)
  if (!Number.isFinite(interval) || interval < 5) {
    return 'Interval must be at least 5 seconds'
  }
  const timeout = Number(s.timeout_s)
  if (!Number.isFinite(timeout) || timeout < 1) {
    return 'Timeout must be at least 1 second'
  }
  const threshold = Number(s.failure_threshold)
  if (!Number.isInteger(threshold) || threshold < 1) {
    return 'Failure threshold must be a positive integer'
  }
  if (s.expected_status.length > 0) {
    const code = Number(s.expected_status)
    if (!Number.isInteger(code) || code < 100 || code > 599) {
      return 'Expected status must be a valid HTTP status code'
    }
  }
  return null
}

function to_input(s: FormState): MonitorInput {
  return {
    name: s.name.trim(),
    kind: s.kind,
    target: s.target.trim(),
    interval_ms: Number(s.interval_s) * 1000,
    timeout_ms: Number(s.timeout_s) * 1000,
    failure_threshold: Number(s.failure_threshold),
    enabled: s.enabled,
    http_method: s.kind === 'http' ? s.http_method : undefined,
    expected_status:
      s.kind === 'http' && s.expected_status.length > 0
        ? Number(s.expected_status)
        : undefined,
    keyword: s.kind === 'http' && s.keyword.length > 0 ? s.keyword : undefined,
    expected_ip:
      s.kind === 'dns' && s.expected_ip.trim().length > 0
        ? s.expected_ip.trim()
        : undefined,
  }
}

export function MonitorForm({
  monitor,
  submitting,
  submitError,
  onSubmit,
}: {
  monitor?: Monitor
  submitting: boolean
  submitError: string | null
  onSubmit: (input: MonitorInput) => void
}) {
  const [state, setState] = useState<FormState>(() => initial_state(monitor))
  const [validationError, setValidationError] = useState<string | null>(null)

  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setState((prev) => ({ ...prev, [key]: value }))

  function submit(e: FormEvent) {
    e.preventDefault()
    const problem = validate(state)
    setValidationError(problem)
    if (problem == null) onSubmit(to_input(state))
  }

  const target_meta = TARGET_LABEL[state.kind]
  const error = validationError ?? submitError

  return (
    <Card>
      <CardContent className="pt-6">
        <form onSubmit={submit} className="space-y-4 max-w-xl">
          <div className="space-y-2">
            <Label htmlFor="name">Name</Label>
            <Input
              id="name"
              value={state.name}
              onChange={(e) => set('name', e.target.value)}
              placeholder="My API"
              required
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="kind">Type</Label>
            <Select
              id="kind"
              value={state.kind}
              disabled={monitor != null}
              onChange={(e) => set('kind', e.target.value as MonitorKind)}
            >
              <option value="http">HTTP(S)</option>
              <option value="tcp">TCP port</option>
              <option value="ping">Ping (ICMP)</option>
              <option value="dns">DNS resolution</option>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="target">{target_meta.label}</Label>
            <Input
              id="target"
              value={state.target}
              onChange={(e) => set('target', e.target.value)}
              placeholder={target_meta.placeholder}
              required
            />
          </div>

          {state.kind === 'http' && (
            <>
              <div className="space-y-2">
                <Label htmlFor="http_method">HTTP method</Label>
                <Select
                  id="http_method"
                  value={state.http_method}
                  onChange={(e) => set('http_method', e.target.value)}
                >
                  <option value="GET">GET</option>
                  <option value="HEAD">HEAD</option>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="expected_status">Expected status code (empty = any 2xx)</Label>
                <Input
                  id="expected_status"
                  value={state.expected_status}
                  onChange={(e) => set('expected_status', e.target.value)}
                  placeholder="200"
                  inputMode="numeric"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="keyword">Response keyword (optional)</Label>
                <Input
                  id="keyword"
                  value={state.keyword}
                  onChange={(e) => set('keyword', e.target.value)}
                  placeholder="ok"
                />
              </div>
            </>
          )}

          {state.kind === 'dns' && (
            <div className="space-y-2">
              <Label htmlFor="expected_ip">Expected IP (optional)</Label>
              <Input
                id="expected_ip"
                value={state.expected_ip}
                onChange={(e) => set('expected_ip', e.target.value)}
                placeholder="93.184.216.34"
              />
            </div>
          )}

          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor="interval">Interval (seconds)</Label>
              <Input
                id="interval"
                value={state.interval_s}
                onChange={(e) => set('interval_s', e.target.value)}
                inputMode="numeric"
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="timeout">Timeout (seconds)</Label>
              <Input
                id="timeout"
                value={state.timeout_s}
                onChange={(e) => set('timeout_s', e.target.value)}
                inputMode="numeric"
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="threshold">Failure threshold</Label>
              <Input
                id="threshold"
                value={state.failure_threshold}
                onChange={(e) => set('failure_threshold', e.target.value)}
                inputMode="numeric"
                required
              />
            </div>
          </div>

          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={state.enabled}
              onChange={(e) => set('enabled', e.target.checked)}
            />
            Enabled
          </label>

          {error != null && <p className="text-sm text-destructive">{error}</p>}

          <Button type="submit" disabled={submitting}>
            {submitting ? 'Saving...' : monitor != null ? 'Save changes' : 'Create monitor'}
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}
