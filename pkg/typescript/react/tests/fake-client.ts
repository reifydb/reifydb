// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {asFrameResults} from '@reifydb/core';
import type {FrameResults, ShapeNode} from '@reifydb/core';
import type {
    BatchSubscription,
    BatchSubscriptionMember,
    SubscriptionCallbacks,
    SubscriptionConfig
} from '@reifydb/client';
import type {StoreClient} from '@reifydb/store';

export interface Deferred<T> {
    resolve: (value: T) => void;
    reject: (error: Error) => void;
}

export interface BatchSubscribeCall extends Deferred<BatchSubscription> {
    members: BatchSubscriptionMember[];
}

export interface SubscribeCall extends Deferred<string> {
    rql: string;
    params: any;
    shape: ShapeNode | undefined;
    callbacks: SubscriptionCallbacks<any>;
    config: SubscriptionConfig | undefined;
}

export interface UnsubscribeCall extends Deferred<void> {
    subscriptionId: string;
}

export interface RequestCall extends Deferred<unknown[][]> {
    rql: string;
    params: any;
    shapes: readonly ShapeNode[];
}

function deferred<T>(): Deferred<T> & {promise: Promise<T>} {
    let resolve!: (value: T) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return {promise, resolve, reject};
}

export function flush(): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, 0));
}

export class FakeClient implements StoreClient {
    readonly subscribes: SubscribeCall[] = [];
    readonly unsubscribes: UnsubscribeCall[] = [];
    readonly queries: RequestCall[] = [];
    readonly commands: RequestCall[] = [];
    readonly admins: RequestCall[] = [];

    query<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
        const {promise, ...call} = deferred<unknown[][]>();
        this.queries.push({rql, params, shapes, ...call});
        return promise.then(frames => asFrameResults<S>(frames));
    }

    command<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
        const {promise, ...call} = deferred<unknown[][]>();
        this.commands.push({rql, params, shapes, ...call});
        return promise.then(frames => asFrameResults<S>(frames));
    }

    admin<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
        const {promise, ...call} = deferred<unknown[][]>();
        this.admins.push({rql, params, shapes, ...call});
        return promise.then(frames => asFrameResults<S>(frames));
    }

    subscribe<T>(
        rql: string,
        params: any,
        shape: ShapeNode | undefined,
        callbacks: SubscriptionCallbacks<T>,
        config?: SubscriptionConfig
    ): Promise<string> {
        const {promise, ...call} = deferred<string>();
        this.subscribes.push({rql, params, shape, callbacks, config, ...call});
        return promise;
    }

    unsubscribe(subscriptionId: string): Promise<void> {
        const {promise, ...call} = deferred<void>();
        this.unsubscribes.push({subscriptionId, ...call});
        return promise;
    }
}

export class BatchingFakeClient extends FakeClient {
    readonly batches: BatchSubscribeCall[] = [];

    batchSubscribe(members: BatchSubscriptionMember[]): Promise<BatchSubscription> {
        const {promise, ...call} = deferred<BatchSubscription>();
        this.batches.push({members, ...call});
        return promise;
    }
}
