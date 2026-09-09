// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { DurationValue, Option } from '@reifydb/react'
import type { MonitorRow } from '@/store/queries'

export const baseMonitor: MonitorRow = {
  id: 'mon-1',
  name: 'monitor',
  kind: 'http',
  target: 'https://example.com',
  interval: DurationValue.fromMilliseconds(60_000),
  timeout: DurationValue.fromMilliseconds(10_000),
  httpMethod: Option.some('GET'),
  expectedStatus: Option.some(200),
  keyword: Option.none('Utf8'),
  expectedIp: Option.none('Utf8'),
  failureThreshold: 1,
  enabled: true,
  status: 'up',
  createdAt: new Date('2026-01-01T00:00:00Z'),
  lastCheckedAt: Option.none('DateTime'),
  consecutiveFailures: 0,
}
