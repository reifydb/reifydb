// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from 'react'
import { ReifyError, Shape, type InferShape, type Store } from '@reifydb/react'
import type { Me } from '@/lib/types'
import { isAuthError } from '@/store/client'

const ME_RQL = 'map { id: $identity.id, name: $identity.name, kind: $identity.kind }'

const meShape = Shape.object({
  id: Shape.identityid(),
  name: Shape.utf8(),
  kind: Shape.utf8(),
})

type MeRow = InferShape<typeof meShape>

const MAX_RETRIES = 3

const MeContext = createContext<Me | undefined>(undefined)

function isConnectionLost(err: unknown): boolean {
  return err instanceof ReifyError && err.code === 'CONNECTION_LOST'
}

function toMe(row: MeRow): Me {
  const guest = row.kind === 'guest'
  return { id: row.id, email: guest ? null : row.name, guest }
}

export function isGuestSession(store: Store): boolean {
  const entry = store.getEntry(ME_RQL, null, meShape)
  return entry.status === 'ready' && entry.data[0]?.kind === 'guest'
}

export function MeProvider({ store, children }: { store: Store | undefined; children: ReactNode }) {
  const subscribe = useCallback(
    (listener: () => void) => (store == null ? () => undefined : store.subscribeState(listener)),
    [store],
  )
  const entry = useSyncExternalStore(subscribe, () => store?.getEntry(ME_RQL, null, meShape))
  const [failure, setFailure] = useState<Error | undefined>(undefined)
  useEffect(() => {
    if (store == null) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const load = (attempt: number) => {
      store.query(ME_RQL, null, meShape).catch((err: unknown) => {
        if (cancelled || isAuthError(err)) return
        if (isConnectionLost(err) && attempt < MAX_RETRIES) {
          timer = setTimeout(() => load(attempt + 1), 1000 * 2 ** attempt)
          return
        }
        setFailure(err instanceof Error ? err : new Error(String(err)))
      })
    }
    load(0)
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [store])
  if (failure != null) throw failure
  const row = entry?.status === 'ready' ? entry.data[0] : undefined
  const me = useMemo(() => (row == null ? undefined : toMe(row)), [row])
  return createElement(MeContext.Provider, { value: me }, children)
}

export function useMe(): { data: Me | undefined } {
  return { data: useContext(MeContext) }
}
