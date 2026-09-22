// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { ListValue, Shape, Utf8Value, Uuid7Value, rql } from '@reifydb/react'
import type { TestDb } from '@reifydb/reifydb'
import { commandAs, queryRoot } from './db'

const CREATE_STATUS_PAGE = rql.write([])<{
  id: Uuid7Value
  slug: Utf8Value
  title: Utf8Value
}>`CALL uptime::create_status_page($id, $slug, $title)`

const ADD_STATUS_PAGE_MONITORS = rql.write([])<{
  id: Uuid7Value
  monitor_ids: ListValue
}>`CALL uptime::add_status_page_monitors($id, $monitor_ids)`

const pages = rql(
  Shape.object({ id: Shape.uuid7(), slug: Shape.utf8(), title: Shape.utf8() }),
)`from uptime::status_pages map { id, slug, title }`

const membersOfPage = rql(Shape.object({ monitor_id: Shape.uuid7(), position: Shape.int2() }))<{
  id: Uuid7Value
}>`from uptime::status_page_monitors filter { status_page_id == $id } map { monitor_id, position }`

const memberPositions = rql(Shape.object({ position: Shape.int2() }))`from uptime::status_page_monitors map { position }`

export async function createStatusPage(
  db: TestDb,
  identity: string,
  slug: string,
  title: string,
  monitorIds: string[],
): Promise<string> {
  // one command per CALL, so a page written here never shares a code path with the web hooks
  const id = Uuid7Value.generate()
  await commandAs(db, identity, CREATE_STATUS_PAGE, { id, slug: new Utf8Value(slug), title: new Utf8Value(title) })
  await commandAs(db, identity, ADD_STATUS_PAGE_MONITORS, {
    id,
    monitor_ids: new ListValue(monitorIds.map((monitorId) => new Uuid7Value(monitorId)), 'Uuid7'),
  })
  return id.toString()
}

export async function readPages(db: TestDb): Promise<{ id: string; slug: string; title: string }[]> {
  const rows = await queryRoot(db, pages, null)
  return rows.map(({ id, slug, title }) => ({ id, slug, title })).sort((a, b) => a.slug.localeCompare(b.slug))
}

export async function readMembers(
  db: TestDb,
  pageId: string,
): Promise<{ monitorId: string; position: number }[]> {
  const rows = await queryRoot(db, membersOfPage, { id: new Uuid7Value(pageId) })
  return rows.map(({ monitorId, position }) => ({ monitorId, position })).sort((a, b) => a.position - b.position)
}

export async function countMembers(db: TestDb): Promise<number> {
  const rows = await queryRoot(db, memberPositions, null)
  return rows.length
}
