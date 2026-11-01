// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useMemo } from 'react'
import { Activity, EDIT_RQL } from '@/activity'
import { type CanvasClient, bytes, numeric, text } from '@/client'
import { Minimap } from '@/minimap'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'

export interface Live {
  buffer: PixelBuffer
  minimap: Minimap
  activity: Activity
}

export function useLive(client: CanvasClient, onError: (message: string) => void): Live {
  const live = useMemo(() => ({ buffer: new PixelBuffer(), minimap: new Minimap(), activity: new Activity() }), [])
  const stream = useMemo(
    () =>
      new TileStream(
        client,
        {
          put: (tx, ty, pixels, version) => {
            if (!live.buffer.put(tx, ty, pixels, version)) return
            live.minimap.put(tx, ty, pixels)
          },
        },
        () => onError('Live updates failed: the tile stream was closed.'),
      ),
    [client, live, onError],
  )

  useEffect(() => {
    stream.start().catch((error: unknown) => {
      onError(`Live updates failed: ${error instanceof Error ? error.message : String(error)}`)
    })
    return () => void stream.close()
  }, [stream, onError])

  useEffect(() => {
    let closed = false
    let ids: string[] = []
    client
      .subscribe([
        {
          rql: EDIT_RQL,
          params: {},
          config: { hydration: { enabled: false } },
          callbacks: {
            onInsert: (rows: Record<string, unknown>[]) => {
              for (const row of rows) live.activity.put(text(row.name), bytes(row.pixels), numeric(row.painted))
            },
            onClosed: () => onError('Live updates failed: the edit stream was closed.'),
          },
        },
      ])
      .then(
        (subscribed) => {
          if (closed) void client.unsubscribe(subscribed)
          else ids = subscribed
        },
        (error: unknown) => onError(`Live updates failed: ${error instanceof Error ? error.message : String(error)}`),
      )
    return () => {
      closed = true
      if (ids.length > 0) void client.unsubscribe(ids)
    }
  }, [client, live, onError])

  return live
}
