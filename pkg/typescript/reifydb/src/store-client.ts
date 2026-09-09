// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { StoreClient } from '@reifydb/store'
import type { Db } from './db'

const NO_SUBSCRIPTIONS = 'the native bridge has no subscription support; subscribe and unsubscribe are unavailable on storeClient(db)'

export function storeClient(db: Db): StoreClient {
  return {
    query: (rql, params, shapes) => db.queryRoot(rql, params, shapes),
    command: (rql, params, shapes) => db.commandRoot(rql, params, shapes),
    admin: (rql, params, shapes) => db.adminRoot(rql, params, shapes),
    subscribe: () => Promise.reject(new Error(NO_SUBSCRIPTIONS)),
    unsubscribe: () => Promise.reject(new Error(NO_SUBSCRIPTIONS)),
  }
}
