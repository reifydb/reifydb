// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useMemo } from 'react';
import { ShapeNode, InferShape } from '@reifydb/core';
import type { SubscriptionConfig } from '@reifydb/client';
import {
    useSubscriptionExecutor,
    type SubscriptionExecutorOptions,
    type ChangeEvent
} from './use-subscription-executor';

export interface SubscriptionOptions extends SubscriptionExecutorOptions {
    enabled?: boolean;  // Auto-subscribe (default: true)
    config?: SubscriptionConfig;
}

export function useSubscription<S extends ShapeNode = any>(
    rql: string,
    params?: any,
    shape?: S,
    options?: SubscriptionOptions
): {
    data: InferShape<S>[];
    changes: ChangeEvent<InferShape<S>>[];
    isSubscribed: boolean;
    isSubscribing: boolean;
    error: string | undefined;
    subscriptionId: string | undefined;
} {
    const {
        state,
        subscribe,
        unsubscribe
    } = useSubscriptionExecutor<InferShape<S>>(options);

    // Serialize params for stable comparison (objects create new refs each render)
    const paramsKey = useMemo(() => JSON.stringify(params), [params]);
    const configKey = useMemo(() => JSON.stringify(options?.config), [options?.config]);

    useEffect(() => {
        if (options?.enabled === false) return;

        subscribe(rql, params, shape, options?.config);

        return () => {
            unsubscribe();
        };
    }, [rql, paramsKey, shape, configKey, options?.enabled, subscribe, unsubscribe]);

    return {
        data: state.data,
        changes: state.changes,
        isSubscribed: state.isSubscribed,
        isSubscribing: state.isSubscribing,
        error: state.error,
        subscriptionId: state.subscriptionId
    };
}
