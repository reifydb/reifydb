// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useEffect} from 'react';
import type {ShapeNode} from '@reifydb/core';
import {entryKey} from '@reifydb/store';
import type {Entry, ReadSpec, SpecData} from '@reifydb/store';
import {useStore} from './provider';
import {useEntry} from './use-entry';

export interface UseSubscriptionOptions {
    enabled?: boolean;
}

export function useSubscription<S extends ShapeNode, P extends object | null>(
    spec: ReadSpec<S, P>,
    params: NoInfer<P>,
    options: UseSubscriptionOptions = {}
): Entry<SpecData<ReadSpec<S, P>>> {
    const store = useStore();
    const {enabled = true} = options;
    const key = entryKey(spec.rql, params, spec.shape);
    useEffect(() => {
        if (!enabled) {
            return;
        }
        return store.subscribe(spec, params);
    }, [store, key, enabled]);
    return useEntry(store, spec, params);
}
