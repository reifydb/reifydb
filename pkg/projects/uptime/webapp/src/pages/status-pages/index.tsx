// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link } from '@tanstack/react-router'
import { ExternalLink, Pencil, Plus, Trash2 } from 'lucide-react'
import { useDeleteStatusPage, useStatusPages } from '@/hooks/use-status-pages'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

export function StatusPagesPage() {
  const { data: pages, isLoading, error } = useStatusPages()
  const remove = useDeleteStatusPage()

  function delete_page(id: string, title: string) {
    if (!window.confirm(`Delete status page "${title}"?`)) return
    remove.mutate(id)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Status pages</h1>
        <Link to="/status-pages/new">
          <Button>
            <Plus className="h-4 w-4" />
            New status page
          </Button>
        </Link>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Loading...</p>}
      {error != null && (
        <p className="text-sm text-destructive">Failed to load status pages: {error.message}</p>
      )}

      {pages != null && pages.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center space-y-4">
            <p className="text-muted-foreground">
              No status pages yet. Publish a public page for a set of your monitors.
            </p>
            <Link to="/status-pages/new">
              <Button>
                <Plus className="h-4 w-4" />
                Create status page
              </Button>
            </Link>
          </CardContent>
        </Card>
      )}

      {pages != null && pages.length > 0 && (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Title</TableHead>
                <TableHead>Public URL</TableHead>
                <TableHead>Monitors</TableHead>
                <TableHead className="w-32" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {pages.map((p) => (
                <TableRow key={p.id}>
                  <TableCell className="font-medium">{p.title}</TableCell>
                  <TableCell>
                    <a
                      href={`/status/${p.slug}`}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-1 text-primary hover:underline"
                    >
                      /status/{p.slug}
                      <ExternalLink className="h-3 w-3" />
                    </a>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {p.monitor_ids.length}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-1 justify-end">
                      <Link to="/status-pages/$pageId/edit" params={{ pageId: p.id }}>
                        <Button variant="ghost" size="icon">
                          <Pencil className="h-4 w-4" />
                        </Button>
                      </Link>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => delete_page(p.id, p.title)}
                        disabled={remove.isPending}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  )
}
