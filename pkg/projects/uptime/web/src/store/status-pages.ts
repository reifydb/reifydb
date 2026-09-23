// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Shape, rql, type InferShape } from '@reifydb/react'

export const statusPages = rql(
  Shape.object({
    id: Shape.uuid7(),
    slug: Shape.utf8(),
    title: Shape.utf8(),
    created_at: Shape.datetime(),
  }),
)`from uptime::status_pages`.options({ config: { hydration: { enabled: true, maxRows: 1000 } } })

export type StatusPageRow = InferShape<typeof statusPages.shape>

export const statusPageMonitors = rql(
  Shape.object({
    status_page_id: Shape.uuid7(),
    monitor_id: Shape.uuid7(),
    position: Shape.int2(),
  }),
)`from uptime::status_page_monitors`.options({ config: { hydration: { enabled: true, maxRows: 10000 } } })

export type StatusPageMonitorRow = InferShape<typeof statusPageMonitors.shape>
