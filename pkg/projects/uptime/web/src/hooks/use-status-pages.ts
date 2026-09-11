// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useMemo } from 'react'
import { useCommand, useSubscription, Utf8Value, Uuid7Value } from '@reifydb/react'
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

const CREATE_STATUS_PAGE = 'CALL uptime::create_status_page($id, $slug, $title)'

const UPDATE_STATUS_PAGE =
  'CALL uptime::update_status_page($id, $slug, $title); CALL uptime::clear_status_page_monitors($id)'

const DELETE_STATUS_PAGE = 'CALL uptime::delete_status_page($id)'

const CHECK_STATUS_PAGE_MONITORS = 'CALL uptime::check_status_page_monitors($id)'

const NO_FRAMES = [] as const

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

function withMembers(head: string, id: string, input: StatusPageInput): [string, Record<string, unknown>] {
  const params: Record<string, unknown> = {
    id: new Uuid7Value(id),
    slug: new Utf8Value(input.slug),
    title: new Utf8Value(input.title),
  }
  const calls = [head]
  input.monitor_ids.forEach((monitorId, position) => {
    params[`monitor_${position}`] = new Uuid7Value(monitorId)
    calls.push(`CALL uptime::add_status_page_monitor($id, $monitor_${position}, ${position})`)
  })
  calls.push(CHECK_STATUS_PAGE_MONITORS)
  return [calls.join('; '), params]
}

export function useStatusPages(): Live<StatusPage[]> {
  const pages = useSubscription(statusPages.rql, null, statusPages.shape, { config: statusPages.config })
  const members = useSubscription(statusPageMonitors.rql, null, statusPageMonitors.shape, {
    config: statusPageMonitors.config,
  })
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
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const create = useCallback(
    async (input: StatusPageInput): Promise<void> => {
      await recorded(run(...withMembers(CREATE_STATUS_PAGE, Uuid7Value.generate().toString(), input)))
    },
    [run],
  )
  return { create, isPending, error }
}

export function useUpdateStatusPage(id: string) {
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const update = useCallback(
    async (input: StatusPageInput): Promise<void> => {
      await recorded(run(...withMembers(UPDATE_STATUS_PAGE, id, input)))
    },
    [run, id],
  )
  return { update, isPending, error }
}

export function useDeleteStatusPage() {
  const { run, isPending, error } = useCommand(NO_FRAMES)
  const remove = useCallback(
    async (id: string): Promise<void> => {
      await recorded(run(DELETE_STATUS_PAGE, { id: new Uuid7Value(id) }))
    },
    [run],
  )
  return { remove, isPending, error }
}
