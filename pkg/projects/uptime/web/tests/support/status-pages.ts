// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Int2Value, Shape, Utf8Value, Uuid7Value, type StoreClient } from '@reifydb/react'
import type { TestDb } from '@reifydb/reifydb'

export async function createStatusPage(
  client: StoreClient,
  slug: string,
  title: string,
  monitorIds: string[],
): Promise<string> {
  // one command per CALL, so a page written here never shares a code path with the web hooks
  const id = Uuid7Value.generate()
  await client.command(
    'CALL uptime::create_status_page($id, $slug, $title)',
    { id, slug: new Utf8Value(slug), title: new Utf8Value(title) },
    [],
  )
  for (const [position, monitorId] of monitorIds.entries()) {
    await client.command(
      'CALL uptime::add_status_page_monitor($id, $monitor_id, $position)',
      { id, monitor_id: new Uuid7Value(monitorId), position: new Int2Value(position) },
      [],
    )
  }
  return id.toString()
}

export async function readPages(db: TestDb): Promise<{ id: string; slug: string; title: string }[]> {
  const [rows] = await db.queryRoot('from uptime::status_pages map { id, slug, title }', {}, [
    Shape.object({ id: Shape.uuid7(), slug: Shape.utf8(), title: Shape.utf8() }),
  ])
  return rows.map(({ '#rownum': _, ...row }) => row).sort((a, b) => a.slug.localeCompare(b.slug))
}

export async function readMembers(
  db: TestDb,
  pageId: string,
): Promise<{ monitorId: string; position: number }[]> {
  const [rows] = await db.queryRoot(
    'from uptime::status_page_monitors filter { status_page_id == $id } map { monitor_id, position }',
    { id: new Uuid7Value(pageId) },
    [Shape.object({ monitor_id: Shape.uuid7(), position: Shape.int2() })],
  )
  return rows.map(({ '#rownum': _, ...row }) => row).sort((a, b) => a.position - b.position)
}

export async function countMembers(db: TestDb): Promise<number> {
  const [rows] = await db.queryRoot('from uptime::status_page_monitors map { position }', {}, [
    Shape.object({ position: Shape.int2() }),
  ])
  return rows.length
}
