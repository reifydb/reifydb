// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useEffect} from 'react';
import type {InferShape, ShapeNode} from '@reifydb/core';
import {entryKey} from '@reifydb/store';
import type {Entry} from '@reifydb/store';
import {useStore} from './provider';
import {useEntry} from './use-entry';

export interface UseQueryOptions {
    enabled?: boolean;
}

export function useQuery<S extends ShapeNode>(
    rql: string,
    params: any,
    shape: S,
    options: UseQueryOptions = {}
): Entry<InferShape<S>> {
    const store = useStore();
    const {enabled = true} = options;
    const key = entryKey(rql, params, shape);
    useEffect(() => {
        if (enabled) {
            // The rejection is already recorded on the entry, so the promise only needs to be settled.
            store.query(rql, params, shape).catch(() => undefined);
        }
    }, [store, key, enabled]);
    return useEntry(store, rql, params, shape) as Entry<InferShape<S>>;
}
