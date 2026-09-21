// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {createStore} from 'zustand/vanilla';
import type {StoreApi} from 'zustand/vanilla';
import type {FrameResults, InferShape, ShapeNode} from '@reifydb/core';
import type {
    BatchSubscribeItem,
    SubscriptionCallbacks,
    SubscriptionConfig,
    SubscriptionRow
} from '@reifydb/client';
import type {StoreClient} from './client';
import {entryKey} from './key';
import {LOADING, indexRows, removeRows, tupleLoading, upsertFrames, upsertRows, withRows, withStatus} from './entry';
import type {Entry} from './entry';
import type {ReadSpec, SpecData, WriteSpec} from './rql';

export interface StoreState {
    entries: Record<string, Entry<unknown[]>>;
}

export interface StoreOptions {
    onBackgroundError?: (error: Error) => void;
    // Opens every subscription that starts in the same tick as one batch. React runs a page's mount
    // effects in one pass, so this is one round trip per page rather than one per hook.
    batch?: boolean;
}

export type Release = () => void;

interface Subscription {
    refcount: number;
    id: string | undefined;
    closed: boolean;
}

interface Queued {
    key: string;
    sub: Subscription;
    request: BatchSubscribeItem;
}

// Infinity never reaches zero, so seeded entries are never released or resubscribed on the client.
const SEEDED = Number.POSITIVE_INFINITY;

// The local infer stops TypeScript inferring S backwards through InferShape, which recurses without limit.
type SeedRows<S extends ShapeNode> = [InferShape<S>] extends [infer R] ? R[] : never;

function toError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
}

export class Store {
    private readonly state: StoreApi<StoreState> = createStore<StoreState>(() => ({entries: {}}));
    private readonly subscriptions = new Map<string, Subscription>();
    private readonly onBackgroundError: (error: Error) => void;
    private readonly batch: boolean;
    private queued: Queued[] = [];
    private flushing = false;

    constructor(private readonly client: StoreClient, options: StoreOptions = {}) {
        this.onBackgroundError = options.onBackgroundError ?? (() => undefined);
        this.batch = options.batch ?? false;
    }

    subscribe<S extends ShapeNode, P extends object | null>(spec: ReadSpec<S, P>, params: NoInfer<P>): Release {
        const key = entryKey(spec.rql, params, spec.shape);
        const sub = this.subscriptions.get(key) ?? this.open(key, spec.rql, params, spec.shape, spec.config);
        sub.refcount += 1;
        let released = false;
        return () => {
            if (released) {
                return;
            }
            released = true;
            sub.refcount -= 1;
            if (sub.refcount === 0) {
                queueMicrotask(() => {
                    if (sub.refcount === 0 && !sub.closed) {
                        this.close(key, sub);
                    }
                });
            }
        };
    }

    async query<S extends ShapeNode | readonly ShapeNode[], P extends object | null>(
        spec: ReadSpec<S, P>,
        params: NoInfer<P>
    ): Promise<SpecData<ReadSpec<S, P>>> {
        const key = entryKey(spec.rql, params, spec.shape);
        const tuple = Array.isArray(spec.shape);
        const shapes = (tuple ? spec.shape : [spec.shape]) as readonly ShapeNode[];
        if (this.state.getState().entries[key] === undefined) {
            this.setEntry(key, tuple ? tupleLoading(shapes.length) : LOADING);
        }
        try {
            const frames = await this.client.query(spec.rql, params, shapes) as SubscriptionRow<unknown>[][];
            this.setEntry(key, tuple ? upsertFrames(frames) : withStatus(upsertRows(LOADING, frames[0]), 'ready'));
            return (tuple ? frames : frames[0]) as SpecData<ReadSpec<S, P>>;
        } catch (error) {
            this.update(key, entry => withStatus(entry, 'error', toError(error)));
            throw error;
        }
    }

    async command<S extends readonly ShapeNode[], P extends object | null>(
        spec: WriteSpec<S, P>,
        params: NoInfer<P>
    ): Promise<FrameResults<S>> {
        const frames = await this.client.command(spec.rql, params, spec.shape);
        return spec.shape.length === 0 ? [] as FrameResults<S> : frames;
    }

    async admin<S extends readonly ShapeNode[], P extends object | null>(
        spec: WriteSpec<S, P>,
        params: NoInfer<P>
    ): Promise<FrameResults<S>> {
        const frames = await this.client.admin(spec.rql, params, spec.shape);
        return spec.shape.length === 0 ? [] as FrameResults<S> : frames;
    }

    getEntry<S extends ShapeNode | readonly ShapeNode[], P extends object | null>(
        spec: ReadSpec<S, P>,
        params: NoInfer<P>
    ): Entry<SpecData<ReadSpec<S, P>>> {
        const entry = this.state.getState().entries[entryKey(spec.rql, params, spec.shape)]
            ?? (Array.isArray(spec.shape) ? tupleLoading(spec.shape.length) : LOADING);
        return entry as Entry<SpecData<ReadSpec<S, P>>>;
    }

    subscribeState(listener: () => void): () => void {
        return this.state.subscribe(listener);
    }

    getSnapshot(): StoreState {
        return this.state.getState();
    }

    seed<S extends ShapeNode, P extends object | null>(spec: ReadSpec<S, P>, params: NoInfer<P>, rows: SeedRows<S>): void {
        const key = entryKey(spec.rql, params, spec.shape);
        this.subscriptions.set(key, {refcount: SEEDED, id: undefined, closed: false});
        this.setEntry(key, withStatus(withRows(LOADING, indexRows(rows)), 'ready'));
    }

    fail<S extends ShapeNode, P extends object | null>(spec: ReadSpec<S, P>, params: NoInfer<P>, error: Error): void {
        const key = entryKey(spec.rql, params, spec.shape);
        this.subscriptions.set(key, {refcount: SEEDED, id: undefined, closed: false});
        this.setEntry(key, withStatus(this.state.getState().entries[key] ?? LOADING, 'error', error));
    }

    reset(): void {
        for (const [key, sub] of this.subscriptions) {
            this.close(key, sub);
        }
        this.state.setState({entries: {}});
    }

    private open(key: string, rql: string, params: any, shape: ShapeNode, config?: SubscriptionConfig): Subscription {
        const sub: Subscription = {refcount: 0, id: undefined, closed: false};
        this.subscriptions.set(key, sub);
        this.setEntry(key, LOADING);
        const merge = (rows: SubscriptionRow<unknown>[]) => {
            if (!sub.closed) {
                this.update(key, entry => upsertRows(entry, rows));
            }
        };
        const callbacks: SubscriptionCallbacks<unknown> = {
            onInsert: merge,
            onUpdate: merge,
            onRemove: rows => {
                if (!sub.closed) {
                    this.update(key, entry => removeRows(entry, rows));
                }
            },
            // A change the client cannot decode leaves the entry stale, so it must not stay 'ready'.
            onError: error => {
                if (!sub.closed) {
                    this.update(key, entry => withStatus(entry, 'error', toError(error)));
                }
            },
            onResubscribe: id => this.attach(key, sub, id, new Map()),
        };
        if (this.batch && this.client.batchSubscribe !== undefined) {
            this.queued.push({key, sub, request: {rql, params, shape, callbacks, config}});
            this.scheduleFlush();
            return sub;
        }
        this.client.subscribe(rql, params, shape, callbacks, config).then(
            id => this.attach(key, sub, id),
            error => this.reject(key, sub, error)
        );
        return sub;
    }

    private scheduleFlush(): void {
        if (this.flushing) {
            return;
        }
        this.flushing = true;
        queueMicrotask(() => {
            this.flushing = false;
            this.flush();
        });
    }

    private flush(): void {
        const queued = this.queued;
        this.queued = [];
        // A subscription released before the batch went out was never opened, so there is nothing to
        // send for it and nothing to unsubscribe.
        const live = queued.filter(entry => !entry.sub.closed);
        if (live.length === 0) {
            return;
        }
        const batchSubscribe = this.client.batchSubscribe;
        if (batchSubscribe === undefined) {
            for (const {key, sub} of live) {
                this.reject(key, sub, new Error('the client stopped offering batchSubscribe'));
            }
            return;
        }
        batchSubscribe.call(this.client, live.map(entry => entry.request)).then(
            ({subscriptionIds}) => {
                live.forEach(({key, sub}, index) => {
                    const id = subscriptionIds[index];
                    if (id === undefined) {
                        this.reject(key, sub, new Error('the batch ack named no id for this subscription'));
                        return;
                    }
                    this.attach(key, sub, id);
                });
            },
            error => {
                for (const {key, sub} of live) {
                    this.reject(key, sub, error);
                }
            }
        );
    }

    private attach(key: string, sub: Subscription, id: string, rows?: Map<number, unknown>): void {
        if (sub.closed) {
            this.client.unsubscribe(id).catch(error => this.onBackgroundError(toError(error)));
            return;
        }
        sub.id = id;
        this.update(key, entry => withStatus(rows === undefined ? entry : withRows(entry, rows), 'ready'));
    }

    private reject(key: string, sub: Subscription, error: unknown): void {
        if (!sub.closed) {
            this.update(key, entry => withStatus(entry, 'error', toError(error)));
        }
    }

    private close(key: string, sub: Subscription): void {
        sub.closed = true;
        this.subscriptions.delete(key);
        const {[key]: removed, ...entries} = this.state.getState().entries;
        if (removed !== undefined) {
            this.state.setState({entries});
        }
        if (sub.id !== undefined) {
            this.client.unsubscribe(sub.id).catch(error => this.onBackgroundError(toError(error)));
        }
    }

    private setEntry(key: string, entry: Entry<unknown[]> | Entry<unknown[][]>): void {
        this.state.setState({entries: {...this.state.getState().entries, [key]: entry as Entry<unknown[]>}});
    }

    private update(key: string, fn: (entry: Entry<unknown[]>) => Entry<unknown[]>): void {
        const entries = this.state.getState().entries;
        const current = entries[key];
        if (current === undefined) {
            return;
        }
        const next = fn(current);
        if (next !== current) {
            this.state.setState({entries: {...entries, [key]: next}});
        }
    }
}
