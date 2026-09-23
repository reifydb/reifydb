// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useCallback, useSyncExternalStore} from 'react';
import type {ShapeNode} from '@reifydb/core';
import type {Entry, ReadSpec, SpecData, Store} from '@reifydb/store';

export function useEntry<S extends ShapeNode | readonly ShapeNode[], P extends object | null>(
    store: Store,
    spec: ReadSpec<S, P>,
    params: P
): Entry<SpecData<ReadSpec<S, P>>> {
    const subscribe = useCallback((listener: () => void) => store.subscribeState(listener), [store]);
    return useSyncExternalStore(subscribe, () => store.getEntry(spec, params));
}
