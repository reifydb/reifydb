// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act } from '@testing-library/react'
import {
  BooleanValue,
  DateTimeValue,
  DurationValue,
  IdentityIdValue,
  Int2Value,
  ListValue,
  NoneValue,
  Option,
  RecordValue,
  Shape,
  Uuid7Value,
  rql,
} from '@reifydb/react'
import type { BridgeClient, TestDb } from '@reifydb/reifydb'
import { commandAs, commandRoot, queryRoot } from './db'
import { routerMock } from './router-mock'

export const routeParams = { monitorId: '' }

export function monitorRouterMock() {
  return { ...routerMock(), useParams: () => routeParams }
}

export interface RegionRef {
  id: string
  label: string
}

const CREATE_MONITOR = rql.write([])<{
  id: string
  name: string
  kind: string
  target: string
  interval: DurationValue
  timeout: DurationValue
  http_method: Option<string>
  expected_status: Option<number>
  keyword: Option<string>
  expected_ip: Option<string>
  failure_threshold: number
  enabled: boolean
  region_ids: ListValue
}>`CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled); CALL uptime::add_monitor_regions($id, $region_ids)`

const REPORT_RESULT = rql.write([])<{
  result_id: string
  monitor_id: string
  owner: IdentityIdValue
  region_id: string
  probe: IdentityIdValue
  checked_at: Date
  success: boolean
  response_time: Option<DurationValue>
  status_code: Option<number>
  error: Option<string>
}>`CALL uptime::report_result($result_id, $monitor_id, $owner, $region_id, $probe, $checked_at, $success, $response_time, $status_code, $error)`

const INSERT_REGION = rql.write([])<{ id: string; label: string }>`INSERT uptime::regions [{ id: $id, label: $label }]`

const INSERT_RESULTS = rql.write([])<{ rows: ListValue }>`INSERT uptime::results $rows`

const identityByName = rql(Shape.object({ id: Shape.identityid() }))<{
  name: string
}>`from system::identities filter { name == $name } map { id }`

const regions = rql(Shape.object({ id: Shape.uuid7(), label: Shape.utf8() }))`from uptime::regions map { id, label }`

export async function caughtUp(client: BridgeClient) {
  await act(async () => {
    // The store sends a batch one microtask after mount; a caughtUp queued before it never waits for hydration.
    await Promise.resolve()
    await client.caughtUp()
  })
}

export async function identityOf(db: TestDb, name: string): Promise<string> {
  const [row] = await queryRoot(db, identityByName, { name })
  if (row == null) throw new Error(`no identity named ${name}`)
  return row.id
}

export async function realRegions(db: TestDb): Promise<RegionRef[]> {
  return queryRoot(db, regions, null)
}

export async function regionNamed(db: TestDb, label: string): Promise<string> {
  const region = (await realRegions(db)).find((r) => r.label === label)
  if (region == null) throw new Error(`no region labelled ${label}`)
  return region.id
}

export async function addRegion(db: TestDb, label: string): Promise<string> {
  const id = Uuid7Value.generate().toString()
  await commandRoot(db, INSERT_REGION, { id, label })
  return id
}

export async function createMonitor(
  db: TestDb,
  identity: string,
  name: string,
  regionIds: string[] = [],
): Promise<string> {
  const id = Uuid7Value.generate().toString()
  await commandAs(db, identity, CREATE_MONITOR, {
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
    region_ids: new ListValue(regionIds.map((regionId) => new Uuid7Value(regionId)), 'Uuid7'),
  })
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
  await commandAs(db, probe, REPORT_RESULT, {
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
  })
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
  const row = () =>
    new RecordValue({
      id: Uuid7Value.generate(),
      monitor_id: new Uuid7Value(results.monitorId),
      owner: new IdentityIdValue(results.owner),
      region_id: new Uuid7Value(results.regionId),
      probe: new NoneValue('IdentityId'),
      checked_at: new DateTimeValue(results.checkedAt),
      success: new BooleanValue(true),
      response_time: new NoneValue('Duration'),
      status_code: new Int2Value(results.statusCode),
      error: new NoneValue('Utf8'),
    })
  await commandRoot(db, INSERT_RESULTS, { rows: new ListValue(Array.from({ length: results.count }, row)) })
}

const monitorById = rql(
  Shape.object({
    name: Shape.utf8(),
    target: Shape.utf8(),
    interval: Shape.durationValue(),
    timeout: Shape.durationValue(),
    enabled: Shape.bool(),
    status: Shape.utf8(),
  }),
)<{ id: string }>`from uptime::monitors filter { id == $id } map { name, target, interval, timeout, enabled, status }`

const monitorRegionsById = rql(Shape.object({ region_id: Shape.uuid7(), status: Shape.utf8() }))<{
  id: string
}>`from uptime::monitor_regions filter { monitor_id == $id } map { region_id, status }`

const resultRegionsById = rql(Shape.object({ region_id: Shape.uuid7() }))<{
  id: string
}>`from uptime::results filter { monitor_id == $id } map { region_id }`

const statusPagesByMonitor = rql(Shape.object({ status_page_id: Shape.uuid7() }))<{
  id: string
}>`from uptime::status_page_monitors filter { monitor_id == $id } map { status_page_id }`

export async function monitorInDb(db: TestDb, id: string) {
  return queryRoot(db, monitorById, { id })
}

export async function monitorRegionsInDb(db: TestDb, id: string): Promise<Record<string, string>> {
  const rows = await queryRoot(db, monitorRegionsById, { id })
  return Object.fromEntries(rows.map((row) => [row.regionId, row.status]))
}

export async function resultRegionsInDb(db: TestDb, id: string): Promise<string[]> {
  const rows = await queryRoot(db, resultRegionsById, { id })
  return rows.map((row) => row.regionId).sort()
}

export async function statusPagesOfMonitorInDb(db: TestDb, id: string): Promise<string[]> {
  const rows = await queryRoot(db, statusPagesByMonitor, { id })
  return rows.map((row) => row.statusPageId)
}
