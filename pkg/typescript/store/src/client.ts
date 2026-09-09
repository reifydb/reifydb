// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {FrameResults, ShapeNode} from '@reifydb/core';
import type {SubscriptionCallbacks, SubscriptionConfig} from '@reifydb/client';

export interface StoreClient {
    query<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>;
    command<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>;
    admin<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>;
    subscribe<T>(
        rql: string,
        params: any,
        shape: ShapeNode | undefined,
        callbacks: SubscriptionCallbacks<T>,
        config?: SubscriptionConfig
    ): Promise<string>;
    unsubscribe(subscriptionId: string): Promise<void>;
}
