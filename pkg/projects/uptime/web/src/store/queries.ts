// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Shape, rql, type InferShape } from '@reifydb/react'

export const monitors = rql(
  Shape.object({
    id: Shape.uuid7(),
    name: Shape.utf8(),
    kind: Shape.utf8(),
    target: Shape.utf8(),
    interval: Shape.durationValue(),
    timeout: Shape.durationValue(),
    http_method: Shape.option(Shape.utf8()),
    expected_status: Shape.option(Shape.int2()),
    keyword: Shape.option(Shape.utf8()),
    expected_ip: Shape.option(Shape.utf8()),
    failure_threshold: Shape.int2(),
    enabled: Shape.bool(),
    status: Shape.utf8(),
    created_at: Shape.datetime(),
    last_checked_at: Shape.option(Shape.datetime()),
    consecutive_failures: Shape.int4(),
  }),
)`from uptime::monitors`.options({ config: { hydration: { enabled: true, maxRows: 1000 } } })

export type MonitorRow = InferShape<typeof monitors.shape>

export const monitorRegions = rql(
  Shape.object({
    monitor_id: Shape.uuid7(),
    region_id: Shape.uuid7(),
    status: Shape.utf8(),
    last_checked_at: Shape.option(Shape.datetime()),
    consecutive_failures: Shape.int4(),
  }),
)`from uptime::monitor_regions`.options({ config: { hydration: { enabled: true, maxRows: 5000 } } })

export type MonitorRegionRow = InferShape<typeof monitorRegions.shape>

export const regions = rql(
  Shape.object({
    id: Shape.uuid7(),
    label: Shape.utf8(),
  }),
)`from uptime::regions`.options({ config: { hydration: { enabled: true, maxRows: 1000 } } })

export type RegionRow = InferShape<typeof regions.shape>

export const results = rql(
  Shape.object({
    region_id: Shape.uuid7(),
    probe: Shape.option(Shape.identityid()),
    checked_at: Shape.datetime(),
    success: Shape.bool(),
    response_time: Shape.option(Shape.durationValue()),
    status_code: Shape.option(Shape.int2()),
    error: Shape.option(Shape.utf8()),
  }),
)<{ monitor_id: string }>`from uptime::results filter { monitor_id == $monitor_id } map { region_id, probe, checked_at, success, response_time, status_code, error } take 200`.options({
  config: { hydration: { enabled: true, maxRows: 200 } },
})

export type ResultRow = InferShape<typeof results.shape>

const dailyShape = Shape.object({
  monitor_id: Shape.uuid7(),
  day: Shape.date(),
  n: Shape.int8(),
})

export type DailyRow = InferShape<typeof dailyShape>

export const dailyTotals = rql(dailyShape)`from uptime::daily_totals`.options({
  config: { hydration: { enabled: true, maxRows: 20000 } },
})

export const dailyUps = rql(dailyShape)`from uptime::daily_ups`.options({
  config: { hydration: { enabled: true, maxRows: 20000 } },
})
