// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef, useState } from 'react'
import { useAuth } from '@reifydb/auth'
import type { Store } from '@reifydb/react'
import { connect, disconnect } from '@/store/client'
import { useEndSession } from './use-sign-out'

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err))
}

export function useSessionStore(): Store | undefined {
  const { session } = useAuth()
  const token = session?.token
  const endSession = useEndSession()
  const endSessionRef = useRef(endSession)
  const [store, setStore] = useState<Store | undefined>(undefined)
  const [failure, setFailure] = useState<Error | undefined>(undefined)

  useEffect(() => {
    endSessionRef.current = endSession
  }, [endSession])

  useEffect(() => {
    let cancelled = false
    const fail = (err: unknown) => setFailure(toError(err))
    if (token) {
      void connect(token, (connected) => {
        void endSessionRef.current(connected).catch(fail)
      }).then((connected) => {
        if (!cancelled) setStore(connected)
      }, fail)
    } else {
      void disconnect()
    }
    return () => {
      cancelled = true
      setStore(undefined)
      void disconnect()
    }
  }, [token])

  if (failure != null) throw failure
  return store
}
