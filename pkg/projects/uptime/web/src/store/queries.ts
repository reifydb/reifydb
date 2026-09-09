// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Shape, type InferShape, type ShapeNode, type SubscriptionConfig } from '@reifydb/react'

interface LiveQuery<S extends ShapeNode> {
  rql: string
  shape: S
  config: SubscriptionConfig
}

const RESULTS_HYDRATION_CAP = 2000

function live<S extends ShapeNode>(rql: string, shape: S, maxRows: number): LiveQuery<S> {
  return { rql, shape, config: { hydration: { enabled: true, maxRows } } }
}

export const monitors = live(
  'from uptime::monitors',
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
  1000,
)

export type MonitorRow = InferShape<typeof monitors.shape>

export const monitorRegions = live(
  'from uptime::monitor_regions',
  Shape.object({
    monitor_id: Shape.uuid7(),
    region_id: Shape.uuid7(),
    status: Shape.utf8(),
    last_checked_at: Shape.option(Shape.datetime()),
    consecutive_failures: Shape.int4(),
  }),
  5000,
)

export type MonitorRegionRow = InferShape<typeof monitorRegions.shape>

export const regions = live(
  'from uptime::regions',
  Shape.object({
    id: Shape.uuid7(),
    label: Shape.utf8(),
  }),
  1000,
)

export type RegionRow = InferShape<typeof regions.shape>

export const results = live(
  `from uptime::results map { monitor_id, region_id, probe, checked_at, success, response_time, status_code, error } take ${RESULTS_HYDRATION_CAP}`,
  Shape.object({
    monitor_id: Shape.uuid7(),
    region_id: Shape.uuid7(),
    probe: Shape.option(Shape.identityid()),
    checked_at: Shape.datetime(),
    success: Shape.bool(),
    response_time: Shape.option(Shape.durationValue()),
    status_code: Shape.option(Shape.int2()),
    error: Shape.option(Shape.utf8()),
  }),
  RESULTS_HYDRATION_CAP,
)

export type ResultRow = InferShape<typeof results.shape>

const dailyShape = Shape.object({
  monitor_id: Shape.uuid7(),
  day: Shape.date(),
  n: Shape.int8(),
})

export type DailyRow = InferShape<typeof dailyShape>

export const dailyTotals = live('from uptime::daily_totals', dailyShape, 20000)

export const dailyUps = live('from uptime::daily_ups', dailyShape, 20000)
