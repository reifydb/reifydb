// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Monitor } from '@/lib/types'

export const baseMonitor: Monitor = {
  id: 'mon-1',
  name: 'monitor',
  kind: 'http',
  target: 'https://example.com',
  interval_ms: 60_000,
  timeout_ms: 10_000,
  http_method: 'GET',
  expected_status: 200,
  keyword: null,
  expected_ip: null,
  failure_threshold: 1,
  enabled: true,
  status: 'up',
  created_at: '2026-01-01T00:00:00Z',
  last_checked_at: null,
}
