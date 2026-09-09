// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {createStore} from 'zustand/vanilla';
import type {StoreApi} from 'zustand/vanilla';
import type {FrameResults, InferShape, ShapeNode} from '@reifydb/core';
import type {SubscriptionCallbacks, SubscriptionConfig, SubscriptionRow} from '@reifydb/client';
import type {StoreClient} from './client';
import {entryKey} from './key';
import {LOADING, indexRows, removeRows, upsertRows, withRows, withStatus} from './entry';
import type {Entry} from './entry';

export interface StoreState {
    entries: Record<string, Entry<unknown>>;
}

export interface StoreOptions {
    onBackgroundError?: (error: Error) => void;
}

export type Release = () => void;

interface Subscription {
    refcount: number;
    id: string | undefined;
    closed: boolean;
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

    constructor(private readonly client: StoreClient, options: StoreOptions = {}) {
        this.onBackgroundError = options.onBackgroundError ?? (() => undefined);
    }

    subscribe<S extends ShapeNode>(rql: string, params: any, shape: S, config?: SubscriptionConfig): Release {
        const key = entryKey(rql, params, shape);
        const sub = this.subscriptions.get(key) ?? this.open(key, rql, params, shape, config);
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

    async query<S extends ShapeNode>(rql: string, params: any, shape: S): Promise<InferShape<S>[]> {
        const key = entryKey(rql, params, shape);
        if (this.state.getState().entries[key] === undefined) {
            this.setEntry(key, LOADING);
        }
        try {
            const frames: readonly unknown[][] = await this.client.query(rql, params, [shape] as readonly ShapeNode[]);
            const rows = frames[0] as InferShape<S>[];
            this.setEntry(key, withStatus(withRows(LOADING, indexRows(rows)), 'ready'));
            return rows;
        } catch (error) {
            this.update(key, entry => withStatus(entry, 'error', toError(error)));
            throw error;
        }
    }

    command<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
        return this.client.command(rql, params, shapes);
    }

    admin<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
        return this.client.admin(rql, params, shapes);
    }

    getEntry<S extends ShapeNode>(rql: string, params: any, shape: S): Entry<InferShape<S>> {
        const entry = this.state.getState().entries[entryKey(rql, params, shape)] ?? LOADING;
        return entry as Entry<InferShape<S>>;
    }

    subscribeState(listener: () => void): () => void {
        return this.state.subscribe(listener);
    }

    getSnapshot(): StoreState {
        return this.state.getState();
    }

    seed<S extends ShapeNode>(rql: string, params: any, shape: S, rows: SeedRows<S>): void {
        const key = entryKey(rql, params, shape);
        this.subscriptions.set(key, {refcount: SEEDED, id: undefined, closed: false});
        this.setEntry(key, withStatus(withRows(LOADING, indexRows(rows)), 'ready'));
    }

    fail(rql: string, params: any, shape: ShapeNode, error: Error): void {
        const key = entryKey(rql, params, shape);
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
        };
        this.client.subscribe(rql, params, shape, callbacks, config).then(
            id => {
                if (sub.closed) {
                    this.client.unsubscribe(id).catch(error => this.onBackgroundError(toError(error)));
                    return;
                }
                sub.id = id;
                this.update(key, entry => withStatus(entry, 'ready'));
            },
            error => {
                if (!sub.closed) {
                    this.update(key, entry => withStatus(entry, 'error', toError(error)));
                }
            }
        );
        return sub;
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

    private setEntry(key: string, entry: Entry<unknown>): void {
        this.state.setState({entries: {...this.state.getState().entries, [key]: entry}});
    }

    private update(key: string, fn: (entry: Entry<unknown>) => Entry<unknown>): void {
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
