// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useCallback, useSyncExternalStore} from 'react';
import type {ShapeNode} from '@reifydb/core';
import type {Entry, Store} from '@reifydb/store';

// Widened to Entry<unknown>: relating Entry<InferShape<S>> to itself for a generic S makes TypeScript recurse through InferShape.
export function useEntry<S extends ShapeNode>(store: Store, rql: string, params: any, shape: S): Entry<unknown> {
    const subscribe = useCallback((listener: () => void) => store.subscribeState(listener), [store]);
    return useSyncExternalStore<Entry<unknown>>(subscribe, () => store.getEntry(rql, params, shape));
}
