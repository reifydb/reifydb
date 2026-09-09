// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useEffect} from 'react';
import type {InferShape, ShapeNode} from '@reifydb/core';
import type {SubscriptionConfig} from '@reifydb/client';
import {entryKey} from '@reifydb/store';
import type {Entry} from '@reifydb/store';
import {useStore} from './provider';
import {useEntry} from './use-entry';

export interface UseSubscriptionOptions {
    config?: SubscriptionConfig;
    enabled?: boolean;
}

export function useSubscription<S extends ShapeNode>(
    rql: string,
    params: any,
    shape: S,
    options: UseSubscriptionOptions = {}
): Entry<InferShape<S>> {
    const store = useStore();
    const {config, enabled = true} = options;
    const key = entryKey(rql, params, shape);
    useEffect(() => {
        if (!enabled) {
            return;
        }
        return store.subscribe(rql, params, shape, config);
    }, [store, key, enabled]);
    return useEntry(store, rql, params, shape) as Entry<InferShape<S>>;
}
