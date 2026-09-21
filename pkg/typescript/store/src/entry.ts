// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {ROW_NUMBER_KEY} from '@reifydb/core';
import type {SubscriptionRow} from '@reifydb/client';

export type EntryStatus = 'loading' | 'ready' | 'error';

export type EntryRows<D> = D extends readonly unknown[][]
    ? {[K in keyof D]: D[K] extends readonly (infer T)[] ? ReadonlyMap<number, T> : never}
    : D extends readonly (infer T)[] ? ReadonlyMap<number, T> : never;

export interface Entry<D> {
    status: EntryStatus;
    rows: EntryRows<D>;
    data: D;
    error: Error | undefined;
}

export const LOADING: Entry<unknown[]> = {status: 'loading', rows: new Map<number, unknown>(), data: [], error: undefined};

const tupleLoadings = new Map<number, Entry<unknown[][]>>();

export function tupleLoading(count: number): Entry<unknown[][]> {
    const cached = tupleLoadings.get(count);
    if (cached !== undefined) {
        return cached;
    }
    const entry: Entry<unknown[][]> = {
        status: 'loading',
        rows: Array.from({length: count}, () => new Map<number, unknown>()),
        data: Array.from({length: count}, () => []),
        error: undefined,
    };
    tupleLoadings.set(count, entry);
    return entry;
}

export function withRows(entry: Entry<unknown[]>, rows: Map<number, unknown>): Entry<unknown[]> {
    return {...entry, rows, data: Array.from(rows.values())};
}

export function withStatus(entry: Entry<unknown[]>, status: EntryStatus, error?: Error): Entry<unknown[]> {
    return {...entry, status, error};
}

export function upsertRows<T>(entry: Entry<unknown[]>, rows: SubscriptionRow<T>[]): Entry<unknown[]> {
    const next = new Map(entry.rows);
    for (const row of rows) {
        const {[ROW_NUMBER_KEY]: rownum, ...data} = row;
        next.set(rownum, data);
    }
    return withRows(entry, next);
}

export function removeRows<T>(entry: Entry<unknown[]>, rows: SubscriptionRow<T>[]): Entry<unknown[]> {
    const next = new Map(entry.rows);
    let changed = false;
    for (const row of rows) {
        changed = next.delete(row[ROW_NUMBER_KEY]) || changed;
    }
    return changed ? withRows(entry, next) : entry;
}

export function upsertFrames(frames: SubscriptionRow<unknown>[][]): Entry<unknown[][]> {
    const entries = frames.map(rows => upsertRows(LOADING, rows));
    return {
        status: 'ready',
        rows: entries.map(entry => entry.rows),
        data: entries.map(entry => entry.data),
        error: undefined,
    };
}
