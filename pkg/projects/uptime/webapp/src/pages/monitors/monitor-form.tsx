// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type FormEvent } from 'react'
import type { Monitor, MonitorInput, MonitorKind } from '@/lib/types'
import { Button, Card, CardContent, Input, Select } from '@reifydb/ui'

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
      <CardContent>
        <form onSubmit={submit} className="space-y-4 max-w-xl">
          <Input
            id="name"
            label="Name"
            value={state.name}
            onChange={(e) => set('name', e.target.value)}
            placeholder="My API"
            required
          />

          <Select
            id="kind"
            label="Type"
            value={state.kind}
            disabled={monitor != null}
            onChange={(e) => set('kind', e.target.value as MonitorKind)}
            options={[
              { value: 'http', label: 'HTTP(S)' },
              { value: 'tcp', label: 'TCP port' },
              { value: 'ping', label: 'Ping (ICMP)' },
              { value: 'dns', label: 'DNS resolution' },
            ]}
          />

          <Input
            id="target"
            label={target_meta.label}
            value={state.target}
            onChange={(e) => set('target', e.target.value)}
            placeholder={target_meta.placeholder}
            required
          />

          {state.kind === 'http' && (
            <>
              <Select
                id="http_method"
                label="HTTP method"
                value={state.http_method}
                onChange={(e) => set('http_method', e.target.value)}
                options={[
                  { value: 'GET', label: 'GET' },
                  { value: 'HEAD', label: 'HEAD' },
                ]}
              />
              <Input
                id="expected_status"
                label="Expected status code (empty = any 2xx)"
                value={state.expected_status}
                onChange={(e) => set('expected_status', e.target.value)}
                placeholder="200"
                inputMode="numeric"
              />
              <Input
                id="keyword"
                label="Response keyword (optional)"
                value={state.keyword}
                onChange={(e) => set('keyword', e.target.value)}
                placeholder="ok"
              />
            </>
          )}

          {state.kind === 'dns' && (
            <Input
              id="expected_ip"
              label="Expected IP (optional)"
              value={state.expected_ip}
              onChange={(e) => set('expected_ip', e.target.value)}
              placeholder="93.184.216.34"
            />
          )}

          <div className="grid grid-cols-3 gap-4">
            <Input
              id="interval"
              label="Interval (seconds)"
              value={state.interval_s}
              onChange={(e) => set('interval_s', e.target.value)}
              inputMode="numeric"
              required
            />
            <Input
              id="timeout"
              label="Timeout (seconds)"
              value={state.timeout_s}
              onChange={(e) => set('timeout_s', e.target.value)}
              inputMode="numeric"
              required
            />
            <Input
              id="threshold"
              label="Failure threshold"
              value={state.failure_threshold}
              onChange={(e) => set('failure_threshold', e.target.value)}
              inputMode="numeric"
              required
            />
          </div>

          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              className="accent-primary"
              checked={state.enabled}
              onChange={(e) => set('enabled', e.target.checked)}
            />
            Enabled
          </label>

          {error != null && <p className="text-sm text-status-error">{error}</p>}

          <Button type="submit" disabled={submitting}>
            {submitting ? 'Saving...' : monitor != null ? 'Save changes' : 'Create monitor'}
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}
