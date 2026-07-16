// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type FormEvent } from 'react'
import { useNavigate, useParams } from '@tanstack/react-router'
import {
  useCreateStatusPage,
  useStatusPage,
  useUpdateStatusPage,
} from '@/hooks/use-status-pages'
import { useMonitors } from '@/hooks/use-monitors'
import type { StatusPage, StatusPageInput } from '@/lib/types'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64)
}

function StatusPageForm({
  page,
  submitting,
  submitError,
  onSubmit,
}: {
  page?: StatusPage
  submitting: boolean
  submitError: string | null
  onSubmit: (input: StatusPageInput) => void
}) {
  const { data: monitors } = useMonitors()
  const [title, setTitle] = useState(page?.title ?? '')
  const [slug, setSlug] = useState(page?.slug ?? '')
  const [slugTouched, setSlugTouched] = useState(page != null)
  const [selected, setSelected] = useState<Set<string>>(
    () => new Set(page?.monitor_ids ?? []),
  )
  const [validationError, setValidationError] = useState<string | null>(null)

  function set_title(value: string) {
    setTitle(value)
    if (!slugTouched) setSlug(slugify(value))
  }

  function toggle(id: string) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  function submit(e: FormEvent) {
    e.preventDefault()
    if (title.trim().length === 0) {
      setValidationError('Title is required')
      return
    }
    if (!/^[a-z0-9][a-z0-9-]*$/.test(slug)) {
      setValidationError('Slug must contain only lowercase letters, digits, and hyphens')
      return
    }
    if (selected.size === 0) {
      setValidationError('Select at least one monitor')
      return
    }
    setValidationError(null)
    onSubmit({ title: title.trim(), slug, monitor_ids: [...selected] })
  }

  const error = validationError ?? submitError

  return (
    <Card>
      <CardContent className="pt-6">
        <form onSubmit={submit} className="space-y-4 max-w-xl">
          <div className="space-y-2">
            <Label htmlFor="title">Title</Label>
            <Input
              id="title"
              value={title}
              onChange={(e) => set_title(e.target.value)}
              placeholder="My Service Status"
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="slug">Slug</Label>
            <Input
              id="slug"
              value={slug}
              onChange={(e) => {
                setSlugTouched(true)
                setSlug(e.target.value)
              }}
              placeholder="my-service"
              required
            />
            <p className="text-xs text-muted-foreground">
              Public URL: /status/{slug || 'my-service'}
            </p>
          </div>
          <div className="space-y-2">
            <Label>Monitors</Label>
            <div className="border border-border divide-y divide-border max-h-64 overflow-auto">
              {(monitors ?? []).map((m) => (
                <label
                  key={m.id}
                  className="flex items-center gap-3 px-3 py-2 text-sm cursor-pointer hover:bg-accent"
                >
                  <input
                    type="checkbox"
                    checked={selected.has(m.id)}
                    onChange={() => toggle(m.id)}
                  />
                  <span className="font-medium">{m.name}</span>
                  <span className="text-muted-foreground text-xs truncate">{m.target}</span>
                </label>
              ))}
              {(monitors ?? []).length === 0 && (
                <p className="px-3 py-4 text-sm text-muted-foreground">
                  No monitors available. Create a monitor first.
                </p>
              )}
            </div>
          </div>

          {error != null && <p className="text-sm text-destructive">{error}</p>}

          <Button type="submit" disabled={submitting}>
            {submitting ? 'Saving...' : page != null ? 'Save changes' : 'Create status page'}
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}

export function StatusPageNewPage() {
  const navigate = useNavigate()
  const create = useCreateStatusPage()

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">New status page</h1>
      <StatusPageForm
        submitting={create.isPending}
        submitError={create.error?.message ?? null}
        onSubmit={(input) =>
          create.mutate(input, {
            onSuccess: () => void navigate({ to: '/status-pages' }),
          })
        }
      />
    </div>
  )
}

export function StatusPageEditPage() {
  const { pageId } = useParams({ strict: false }) as { pageId: string }
  const navigate = useNavigate()
  const { data: page, isLoading, error } = useStatusPage(pageId)
  const update = useUpdateStatusPage(pageId)

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading...</p>
  if (error != null || page == null) {
    return <p className="text-sm text-destructive">Status page not found</p>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Edit {page.title}</h1>
      <StatusPageForm
        page={page}
        submitting={update.isPending}
        submitError={update.error?.message ?? null}
        onSubmit={(input) =>
          update.mutate(input, {
            onSuccess: () => void navigate({ to: '/status-pages' }),
          })
        }
      />
    </div>
  )
}
