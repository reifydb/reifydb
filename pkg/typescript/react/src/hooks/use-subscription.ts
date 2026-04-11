// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, useMemo } from 'react';
import { ShapeNode, InferShape } from '@reifydb/core';
import {
    useSubscriptionExecutor,
    type SubscriptionExecutorOptions,
    type ChangeEvent
} from './use-subscription-executor';

export interface SubscriptionOptions extends SubscriptionExecutorOptions {
    enabled?: boolean;  // Auto-subscribe (default: true)
}

export function useSubscription<S extends ShapeNode = any>(
    query: string,
    params?: any,
    shape?: S,
    options?: SubscriptionOptions
): {
    data: InferShape<S>[];
    changes: ChangeEvent<InferShape<S>>[];
    is_subscribed: boolean;
    is_subscribing: boolean;
    error: string | undefined;
    subscription_id: string | undefined;
} {
    const {
        state,
        subscribe,
        unsubscribe
    } = useSubscriptionExecutor<InferShape<S>>(options);

    // Serialize params for stable comparison (objects create new refs each render)
    const params_key = useMemo(() => JSON.stringify(params), [params]);

    useEffect(() => {
        if (options?.enabled === false) return;

        subscribe(query, params, shape);

        return () => {
            unsubscribe();
        };
    }, [query, params_key, shape, options?.enabled, subscribe, unsubscribe]);

    return {
        data: state.data,
        changes: state.changes,
        is_subscribed: state.is_subscribed,
        is_subscribing: state.is_subscribing,
        error: state.error,
        subscription_id: state.subscription_id
    };
}
