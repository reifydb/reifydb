// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { WsClient } from "../../../src";
import type { ShapeNode, InferShape } from '@reifydb/core';

/**
 * Create a unique test table name to avoid conflicts
 */
export function createTestTableName(prefix: string = 'test'): string {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Helper to create a test table with shape
 * Creates a 'test' namespace if it doesn't exist and uses test::tableName
 */
export async function createTestTable(
    client: WsClient,
    tableName: string,
    columns: string[]
): Promise<void> {
    // Ensure namespace exists
    try {
        await client.admin(`create namespace test`, null, []);
    } catch (err) {
        // Ignore if already exists
    }

    const columnDefs = columns.map(col => {
        const [name, type] = col.split(' ');
        return `${name}: ${type.toLowerCase()}`;
    }).join(', ');

    await client.admin(
        `create table test::${tableName} { ${columnDefs} }`,
        null,
        []
    );
}

/**
 * Wait for a subscription callback to be invoked with timeout
 */
// Overload 1: With shape (type inferred)
// @ts-ignore
export function waitForCallback<S extends ShapeNode>(
    shape: S,
    timeoutMs?: number
): {
    promise: Promise<InferShape<S>[]>,
    callback: (rows: InferShape<S>[]) => void
};

// Overload 2: Without shape (explicit type)
export function waitForCallback<T = any>(
    timeoutMs?: number
): {
    promise: Promise<T[]>,
    callback: (rows: T[]) => void
};

// Implementation
export function waitForCallback<S extends ShapeNode = any>(
    shapeOrTimeout?: S | number,
    timeoutMs: number = 500
): {
    promise: Promise<any[]>,
    callback: (rows: any[]) => void
} {
    // Handle overload parameters
    const timeout = typeof shapeOrTimeout === 'number' ? shapeOrTimeout : timeoutMs;

    let resolve: (rows: any[]) => void;
    let reject: (err: Error) => void;
    let timeoutId: ReturnType<typeof setTimeout>;

    const promise = new Promise<any[]>((res, rej) => {
        resolve = (rows) => {
            clearTimeout(timeoutId);
            res(rows);
        };
        reject = rej;
        timeoutId = setTimeout(() => rej(new Error('Callback timeout')), timeout);
    });

    const callback = (rows: any[]) => {
        resolve(rows);
    };

    return { promise, callback };
}

/**
 * Create a callback tracker for testing multiple invocations
 */
// Overload 1: With shape (type inferred)
export function createCallbackTracker<S extends ShapeNode>(
    shape: S
): {
    callback: (rows: InferShape<S>[]) => void;
    getCalls: () => InferShape<S>[][];
    getCallCount: () => number;
    getAllRows: () => InferShape<S>[];
    clear: () => void;
    waitForCall: (timeoutMs?: number) => Promise<InferShape<S>[]>;
    waitForRows: (count: number, timeoutMs?: number) => Promise<void>;
    waitForRowMatching: (predicate: (row: InferShape<S>) => boolean, timeoutMs?: number) => Promise<InferShape<S>>;
};

// Overload 2: Without shape (explicit type)
export function createCallbackTracker<T = any>(): {
    callback: (rows: T[]) => void;
    getCalls: () => T[][];
    getCallCount: () => number;
    getAllRows: () => T[];
    clear: () => void;
    waitForCall: (timeoutMs?: number) => Promise<T[]>;
    waitForRows: (count: number, timeoutMs?: number) => Promise<void>;
    waitForRowMatching: (predicate: (row: T) => boolean, timeoutMs?: number) => Promise<T>;
};

// Implementation
export function createCallbackTracker<S extends ShapeNode = any>(
    shape?: S
) {
    const calls: any[][] = [];
    let pendingResolve: ((rows: any[]) => void) | null = null;
    // How many recorded calls waitForCall has already handed out. A notification can land before the
    // waiting starts, so waitForCall has to be able to take one that already arrived; the cursor keeps
    // a second wait on the same tracker from being satisfied by the call the first one consumed.
    let consumed = 0;

    return {
        callback: (rows: any[]) => {
            calls.push(rows);
            if (pendingResolve) {
                const fn = pendingResolve;
                pendingResolve = null;
                fn(rows);
            }
        },
        getCalls: () => calls,
        getCallCount: () => calls.length,
        getAllRows: () => calls.flat(),
        clear: () => {
            calls.length = 0;
            consumed = 0;
        },
        waitForCall: (timeoutMs: number = 5000): Promise<any[]> => {
            return new Promise((resolve, reject) => {
                if (consumed < calls.length) {
                    resolve(calls[consumed++]);
                    return;
                }

                const timeout = setTimeout(() => {
                    pendingResolve = null;
                    reject(new Error(`Callback timeout after ${timeoutMs}ms`));
                }, timeoutMs);

                pendingResolve = (rows) => {
                    clearTimeout(timeout);
                    consumed = calls.length;
                    resolve(rows);
                };
            });
        },
        waitForRows: (count: number, timeoutMs: number = 5000): Promise<void> => {
            return new Promise((resolve, reject) => {
                if (calls.flat().length >= count) { resolve(); return; }
                const timeout = setTimeout(() => {
                    pendingResolve = null;
                    reject(new Error(`Timed out waiting for ${count} rows (got ${calls.flat().length}) after ${timeoutMs}ms`));
                }, timeoutMs);
                const check = () => {
                    if (calls.flat().length >= count) {
                        clearTimeout(timeout);
                        pendingResolve = null;
                        resolve();
                    } else {
                        pendingResolve = check;
                    }
                };
                pendingResolve = check;
            });
        },
        // Waits for the row the caller is actually about to assert on. A reconnection replays the rows a
        // subscription already delivered, so "one more callback" can be satisfied by a replay of an
        // earlier row while the awaited one is still in flight.
        waitForRowMatching: (predicate: (row: any) => boolean, timeoutMs: number = 5000): Promise<any> => {
            return new Promise((resolve, reject) => {
                const found = () => calls.flat().find(predicate);
                const already = found();
                if (already !== undefined) {
                    resolve(already);
                    return;
                }
                const timeout = setTimeout(() => {
                    pendingResolve = null;
                    reject(new Error(`Timed out after ${timeoutMs}ms waiting for a matching row`));
                }, timeoutMs);
                const check = () => {
                    const row = found();
                    if (row !== undefined) {
                        clearTimeout(timeout);
                        pendingResolve = null;
                        resolve(row);
                    } else {
                        pendingResolve = check;
                    }
                };
                pendingResolve = check;
            });
        }
    };
}
