// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {ROW_NUMBER_KEY} from '@reifydb/core';
import type {SubscriptionRow} from '@reifydb/client';

export type EntryStatus = 'loading' | 'ready' | 'error';

export interface Entry<T> {
    status: EntryStatus;
    rows: ReadonlyMap<number, T>;
    data: readonly T[];
    error: Error | undefined;
}

export const LOADING: Entry<unknown> = {status: 'loading', rows: new Map<number, unknown>(), data: [], error: undefined};

export function withRows<T>(entry: Entry<T>, rows: Map<number, T>): Entry<T> {
    return {...entry, rows, data: Array.from(rows.values())};
}

export function withStatus<T>(entry: Entry<T>, status: EntryStatus, error?: Error): Entry<T> {
    return {...entry, status, error};
}

export function upsertRows<T>(entry: Entry<T>, rows: SubscriptionRow<T>[]): Entry<T> {
    const next = new Map(entry.rows);
    for (const row of rows) {
        const {[ROW_NUMBER_KEY]: rownum, ...data} = row;
        next.set(rownum, data as T);
    }
    return withRows(entry, next);
}

export function removeRows<T>(entry: Entry<T>, rows: SubscriptionRow<T>[]): Entry<T> {
    const next = new Map(entry.rows);
    let changed = false;
    for (const row of rows) {
        changed = next.delete(row[ROW_NUMBER_KEY]) || changed;
    }
    return changed ? withRows(entry, next) : entry;
}

export function indexRows<T>(rows: readonly T[]): Map<number, T> {
    return new Map(rows.map((row, index) => [index, row]));
}
