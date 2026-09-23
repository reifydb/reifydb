// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useMemo } from 'react'
import { ListValue, rql, useCommand, useSubscription, Utf8Value, Uuid7Value } from '@reifydb/react'
import {
  statusPageMonitors,
  statusPages,
  type StatusPageMonitorRow,
  type StatusPageRow,
} from '@/store/status-pages'
import { recorded } from '@/lib/errors'
import type { StatusPage, StatusPageInput } from '@/lib/types'

interface Live<T> {
  data: T | undefined
  isLoading: boolean
  error: Error | undefined
}

type StatusPageParams = ReturnType<typeof statusPageParams>

const CREATE_STATUS_PAGE = rql.write([])<StatusPageParams>`CALL uptime::create_status_page($id, $slug, $title); CALL uptime::add_status_page_monitors($id, $monitor_ids); CALL uptime::check_status_page_monitors($id)`

const UPDATE_STATUS_PAGE = rql.write([])<StatusPageParams>`CALL uptime::update_status_page($id, $slug, $title); CALL uptime::clear_status_page_monitors($id); CALL uptime::add_status_page_monitors($id, $monitor_ids); CALL uptime::check_status_page_monitors($id)`

const DELETE_STATUS_PAGE = rql.write([])<{ id: Uuid7Value }>`CALL uptime::delete_status_page($id)`

function monitorIdsByPage(rows: readonly StatusPageMonitorRow[]): Map<string, string[]> {
  const byPage = new Map<string, string[]>()
  for (const row of [...rows].sort((a, b) => a.position - b.position)) {
    const ids = byPage.get(row.statusPageId) ?? []
    ids.push(row.monitorId)
    byPage.set(row.statusPageId, ids)
  }
  return byPage
}

function toStatusPage(row: StatusPageRow, monitorIds: Map<string, string[]>): StatusPage {
  return {
    id: row.id,
    slug: row.slug,
    title: row.title,
    created_at: row.createdAt.toISOString(),
    monitor_ids: monitorIds.get(row.id) ?? [],
  }
}

function statusPageParams(id: string, input: StatusPageInput) {
  return {
    id: new Uuid7Value(id),
    slug: new Utf8Value(input.slug),
    title: new Utf8Value(input.title),
    monitor_ids: new ListValue(input.monitor_ids.map((monitorId) => new Uuid7Value(monitorId)), 'Uuid7'),
  }
}

export function useStatusPages(): Live<StatusPage[]> {
  const pages = useSubscription(statusPages, null)
  const members = useSubscription(statusPageMonitors, null)
  return useMemo(() => {
    const error = pages.error ?? members.error
    if (error != null || pages.status !== 'ready' || members.status !== 'ready') {
      return { data: undefined, isLoading: error == null, error }
    }
    const monitorIds = monitorIdsByPage(members.data)
    return {
      data: pages.data
        .map((row) => toStatusPage(row, monitorIds))
        .sort((a, b) => (a.created_at < b.created_at ? 1 : -1)),
      isLoading: false,
      error: undefined,
    }
  }, [pages, members])
}

export function useStatusPage(id: string): Live<StatusPage> {
  const pages = useStatusPages()
  return useMemo(() => ({ ...pages, data: pages.data?.find((p) => p.id === id) }), [pages, id])
}

export function useCreateStatusPage() {
  const { run, isPending, error } = useCommand(CREATE_STATUS_PAGE)
  const create = useCallback(
    async (input: StatusPageInput): Promise<void> => {
      await recorded(run(statusPageParams(Uuid7Value.generate().toString(), input)))
    },
    [run],
  )
  return { create, isPending, error }
}

export function useUpdateStatusPage(id: string) {
  const { run, isPending, error } = useCommand(UPDATE_STATUS_PAGE)
  const update = useCallback(
    async (input: StatusPageInput): Promise<void> => {
      await recorded(run(statusPageParams(id, input)))
    },
    [run, id],
  )
  return { update, isPending, error }
}

export function useDeleteStatusPage() {
  const { run, isPending, error } = useCommand(DELETE_STATUS_PAGE)
  const remove = useCallback(
    async (id: string): Promise<void> => {
      await recorded(run({ id: new Uuid7Value(id) }))
    },
    [run],
  )
  return { remove, isPending, error }
}
