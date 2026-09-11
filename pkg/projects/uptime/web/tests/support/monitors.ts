// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act } from '@testing-library/react'
import { DurationValue, IdentityIdValue, Option, Shape, Uuid7Value } from '@reifydb/react'
import type { BridgeClient, TestDb } from '@reifydb/reifydb'
import { routerMock } from './router-mock'

export const routeParams = { monitorId: '' }

export function monitorRouterMock() {
  return { ...routerMock(), useParams: () => routeParams }
}

export interface RegionRef {
  id: string
  label: string
}

const CREATE_MONITOR =
  'CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)'

const REPORT_RESULT =
  'CALL uptime::report_result($result_id, $monitor_id, $owner, $region_id, $probe, $checked_at, $success, $response_time, $status_code, $error)'

export async function caughtUp(client: BridgeClient) {
  await act(async () => {
    // The store sends a batch one microtask after mount; a caughtUp queued before it never waits for hydration.
    await Promise.resolve()
    await client.caughtUp()
  })
}

export async function identityOf(db: TestDb, name: string): Promise<string> {
  const [[row]] = await db.queryRoot(
    'from system::identities filter { name == $name } map { id }',
    { name },
    [Shape.object({ id: Shape.identityid() })],
  )
  if (row == null) throw new Error(`no identity named ${name}`)
  return row.id
}

export async function realRegions(db: TestDb): Promise<RegionRef[]> {
  const [rows] = await db.queryRoot('from uptime::regions map { id, label }', {}, [
    Shape.object({ id: Shape.uuid7(), label: Shape.utf8() }),
  ])
  return rows
}

export async function regionNamed(db: TestDb, label: string): Promise<string> {
  const region = (await realRegions(db)).find((r) => r.label === label)
  if (region == null) throw new Error(`no region labelled ${label}`)
  return region.id
}

export async function addRegion(db: TestDb, label: string): Promise<string> {
  const id = Uuid7Value.generate().toString()
  await db.commandRoot('INSERT uptime::regions [{ id: $id, label: $label }]', { id, label }, [])
  return id
}

export async function createMonitor(
  client: BridgeClient,
  name: string,
  regionIds: string[] = [],
): Promise<string> {
  const id = Uuid7Value.generate().toString()
  const statements = [
    CREATE_MONITOR,
    ...regionIds.map((_, i) => `CALL uptime::add_monitor_region($id, $region_${i})`),
  ]
  await client.command(
    statements.join('; '),
    {
      id,
      name,
      kind: 'http',
      target: `https://${name}.example.com/health`,
      interval: DurationValue.fromMilliseconds(60_000),
      timeout: DurationValue.fromMilliseconds(10_000),
      http_method: Option.some('GET'),
      expected_status: Option.none('Int2'),
      keyword: Option.none('Utf8'),
      expected_ip: Option.none('Utf8'),
      failure_threshold: 3,
      enabled: true,
      ...Object.fromEntries(regionIds.map((regionId, i) => [`region_${i}`, regionId])),
    },
    [],
  )
  return id
}

export interface ReportedResult {
  monitorId: string
  owner: string
  regionId: string
  success: boolean
  statusCode: number
  responseMs: number
}

export async function reportResult(db: TestDb, result: ReportedResult): Promise<void> {
  const probe = await identityOf(db, 'probe-a')
  await db.commandAs(
    probe,
    REPORT_RESULT,
    {
      result_id: Uuid7Value.generate().toString(),
      monitor_id: result.monitorId,
      owner: new IdentityIdValue(result.owner),
      region_id: result.regionId,
      probe: new IdentityIdValue(probe),
      checked_at: new Date(),
      success: result.success,
      response_time: Option.some(DurationValue.fromMilliseconds(result.responseMs)),
      status_code: Option.some(result.statusCode),
      error: Option.none('Utf8'),
    },
    [],
  )
}

export interface InsertedResults {
  monitorId: string
  owner: string
  regionId: string
  count: number
  statusCode: number
  checkedAt: Date
}

export async function insertResults(db: TestDb, results: InsertedResults): Promise<void> {
  const row =
    '{ id: uuid::v7(), monitor_id: $monitor_id, owner: $owner, region_id: $region_id, probe: none, checked_at: $checked_at, success: true, response_time: none, status_code: $status_code, error: none }'
  await db.commandRoot(
    `INSERT uptime::results [${Array.from({ length: results.count }, () => row).join(', ')}]`,
    {
      monitor_id: results.monitorId,
      owner: new IdentityIdValue(results.owner),
      region_id: results.regionId,
      checked_at: results.checkedAt,
      status_code: Option.some(results.statusCode),
    },
    [],
  )
}

const MONITOR = Shape.object({
  name: Shape.utf8(),
  target: Shape.utf8(),
  interval: Shape.durationValue(),
  timeout: Shape.durationValue(),
  enabled: Shape.bool(),
  status: Shape.utf8(),
})

export async function monitorInDb(db: TestDb, id: string) {
  const [rows] = await db.queryRoot(
    'from uptime::monitors filter { id == $id } map { name, target, interval, timeout, enabled, status }',
    { id },
    [MONITOR],
  )
  return rows
}

export async function monitorRegionsInDb(db: TestDb, id: string): Promise<Record<string, string>> {
  const [rows] = await db.queryRoot(
    'from uptime::monitor_regions filter { monitor_id == $id } map { region_id, status }',
    { id },
    [Shape.object({ region_id: Shape.uuid7(), status: Shape.utf8() })],
  )
  return Object.fromEntries(rows.map((row) => [row.regionId, row.status]))
}

export async function resultRegionsInDb(db: TestDb, id: string): Promise<string[]> {
  const [rows] = await db.queryRoot(
    'from uptime::results filter { monitor_id == $id } map { region_id }',
    { id },
    [Shape.object({ region_id: Shape.uuid7() })],
  )
  return rows.map((row) => row.regionId).sort()
}

export async function statusPagesOfMonitorInDb(db: TestDb, id: string): Promise<string[]> {
  const [rows] = await db.queryRoot(
    'from uptime::status_page_monitors filter { monitor_id == $id } map { status_page_id }',
    { id },
    [Shape.object({ status_page_id: Shape.uuid7() })],
  )
  return rows.map((row) => row.statusPageId)
}
