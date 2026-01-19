// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import { useEffect } from 'react';
import { SchemaNode, InferSchema } from '@reifydb/core';
import {
    useSubscriptionExecutor,
    type SubscriptionExecutorOptions,
    type ChangeEvent
} from './use-subscription-executor';

export interface SubscriptionOptions extends SubscriptionExecutorOptions {
    enabled?: boolean;  // Auto-subscribe (default: true)
}

export function useSubscription<S extends SchemaNode = any>(
    query: string,
    params?: any,
    schema?: S,
    options?: SubscriptionOptions
): {
    data: InferSchema<S>[];
    changes: ChangeEvent<InferSchema<S>>[];
    isSubscribed: boolean;
    isSubscribing: boolean;
    error: string | undefined;
    subscriptionId: string | undefined;
} {
    const {
        state,
        subscribe,
        unsubscribe
    } = useSubscriptionExecutor<InferSchema<S>>(options);

    // Auto-subscribe when query/params change
    useEffect(() => {
        if (options?.enabled === false) return;

        subscribe(query, params, schema);

        return () => {
            unsubscribe();
        };
    }, [query, params, schema, options?.enabled, subscribe, unsubscribe]);

    return {
        data: state.data,
        changes: state.changes,
        isSubscribed: state.isSubscribed,
        isSubscribing: state.isSubscribing,
        error: state.error,
        subscriptionId: state.subscriptionId
    };
}
