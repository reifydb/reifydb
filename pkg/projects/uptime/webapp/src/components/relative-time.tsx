// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useNow } from '@/hooks/use-now'
import { formatRelativeTime } from '@/lib/format'

export function RelativeTime({ iso }: { iso: string | null }) {
  useNow()
  return <>{formatRelativeTime(iso)}</>
}
