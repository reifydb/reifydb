import { useMemo, useEffect } from 'react'
import { useQueryExecutor, useSubscriptionExecutor, useConnection } from '@reifydb/react'

/**
 * Combines an initial query (snapshot) with a subscription (live changes).
 * Subscriptions only deliver change events — they never send existing data.
 * This hook loads existing rows on mount, then merges live INSERT/UPDATE/REMOVE on top.
 */
export function useLiveData(query: string) {
  const { client } = useConnection()
  const { results, isExecuting, query: executeQuery } = useQueryExecutor()
  const { state, subscribe, unsubscribe } = useSubscriptionExecutor()

  useEffect(() => {
    if (!client) return
    executeQuery(query)
  }, [query, client, executeQuery])

  useEffect(() => {
    if (!client) return
    subscribe(query)
    return () => { unsubscribe() }
  }, [query, client, subscribe, unsubscribe])

  const data = useMemo(() => {
    const base = (results?.[0]?.rows ?? []) as any[]
    let merged = [...base]

    for (const change of state.changes) {
      switch (change.operation) {
        case 'INSERT':
          for (const row of change.rows) {
            const rid = (row as any).id
            if (rid != null) {
              if (!merged.some(m => String(m.id) === String(rid))) {
                merged.push(row)
              }
            } else {
              merged.push(row)
            }
          }
          break
        case 'UPDATE':
          for (const row of change.rows) {
            const rid = (row as any).id
            if (rid != null) {
              const idx = merged.findIndex(m => String(m.id) === String(rid))
              if (idx !== -1) {
                merged[idx] = row
              }
            }
          }
          break
        case 'REMOVE':
          for (const row of change.rows) {
            const rid = (row as any).id
            if (rid != null) {
              merged = merged.filter(m => String(m.id) !== String(rid))
            }
          }
          break
      }
    }

    return merged
  }, [results, state.changes])

  return {
    data,
    isExecuting,
    isSubscribed: state.isSubscribed,
    isSubscribing: state.isSubscribing,
    error: state.error
  }
}
