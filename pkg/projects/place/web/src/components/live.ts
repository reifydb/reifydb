// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useMemo } from 'react'
import { Activity } from '@/activity'
import type { PlaceClient } from '@/client'
import { tileKey } from '@/geometry'
import { Minimap } from '@/minimap'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'

export interface Live {
  buffer: PixelBuffer
  minimap: Minimap
  activity: Activity
}

export function useLive(client: PlaceClient, onError: (message: string) => void): Live {
  const live = useMemo(() => ({ buffer: new PixelBuffer(), minimap: new Minimap(), activity: new Activity() }), [])
  const stream = useMemo(
    () =>
      new TileStream(
        client,
        {
          put: (tx, ty, bytes, version) => {
            const before = live.buffer.tile(tileKey(tx, ty))?.bytes
            if (!live.buffer.put(tx, ty, bytes, version)) return
            live.minimap.put(tx, ty, bytes)
            live.activity.put(tx, ty, before, bytes)
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

  return live
}
