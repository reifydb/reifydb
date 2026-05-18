// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { WsClient } from "../../../src";
import type { ShapeNode, InferShape } from '@reifydb/core';

/**
 * Create a unique test table name to avoid conflicts
 */
export function create_test_table_name(prefix: string = 'test'): string {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Helper to create a test table with shape
 * Creates a 'test' namespace if it doesn't exist and uses test::table_name
 */
export async function create_test_table(
    client: WsClient,
    table_name: string,
    columns: string[]
): Promise<void> {
    // Ensure namespace exists
    try {
        await client.admin(`create namespace test`, null, []);
    } catch (err) {
        // Ignore if already exists
    }

    const column_defs = columns.map(col => {
        const [name, type] = col.split(' ');
        return `${name}: ${type.toLowerCase()}`;
    }).join(', ');

    await client.admin(
        `create table test::${table_name} { ${column_defs} }`,
        null,
        []
    );
}

/**
 * Wait for a subscription callback to be invoked with timeout
 */
// Overload 1: With shape (type inferred)
// @ts-ignore
export function wait_for_callback<S extends ShapeNode>(
    shape: S,
    timeout_ms?: number
): {
    promise: Promise<InferShape<S>[]>,
    callback: (rows: InferShape<S>[]) => void
};

// Overload 2: Without shape (explicit type)
export function wait_for_callback<T = any>(
    timeout_ms?: number
): {
    promise: Promise<T[]>,
    callback: (rows: T[]) => void
};

// Implementation
export function wait_for_callback<S extends ShapeNode = any>(
    shape_or_timeout?: S | number,
    timeout_ms: number = 500
): {
    promise: Promise<any[]>,
    callback: (rows: any[]) => void
} {
    // Handle overload parameters
    const timeout = typeof shape_or_timeout === 'number' ? shape_or_timeout : timeout_ms;

    let resolve: (rows: any[]) => void;
    let reject: (err: Error) => void;
    let timeout_id: ReturnType<typeof setTimeout>;

    const promise = new Promise<any[]>((res, rej) => {
        resolve = (rows) => {
            clearTimeout(timeout_id);
            res(rows);
        };
        reject = rej;
        timeout_id = setTimeout(() => rej(new Error('Callback timeout')), timeout);
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
export function create_callback_tracker<S extends ShapeNode>(
    shape: S
): {
    callback: (rows: InferShape<S>[]) => void;
    get_calls: () => InferShape<S>[][];
    get_call_count: () => number;
    get_all_rows: () => InferShape<S>[];
    clear: () => void;
    wait_for_call: (timeout_ms?: number) => Promise<InferShape<S>[]>;
    wait_for_rows: (count: number, timeout_ms?: number) => Promise<void>;
};

// Overload 2: Without shape (explicit type)
export function create_callback_tracker<T = any>(): {
    callback: (rows: T[]) => void;
    get_calls: () => T[][];
    get_call_count: () => number;
    get_all_rows: () => T[];
    clear: () => void;
    wait_for_call: (timeout_ms?: number) => Promise<T[]>;
    wait_for_rows: (count: number, timeout_ms?: number) => Promise<void>;
};

// Implementation
export function create_callback_tracker<S extends ShapeNode = any>(
    shape?: S
) {
    const calls: any[][] = [];
    let pending_resolve: ((rows: any[]) => void) | null = null;

    return {
        callback: (rows: any[]) => {
            calls.push(rows);
            if (pending_resolve) {
                const fn = pending_resolve;
                pending_resolve = null;
                fn(rows);
            }
        },
        get_calls: () => calls,
        get_call_count: () => calls.length,
        get_all_rows: () => calls.flat(),
        clear: () => {
            calls.length = 0;
        },
        wait_for_call: (timeout_ms: number = 5000): Promise<any[]> => {
            return new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    pending_resolve = null;
                    reject(new Error(`Callback timeout after ${timeout_ms}ms`));
                }, timeout_ms);

                pending_resolve = (rows) => {
                    clearTimeout(timeout);
                    resolve(rows);
                };
            });
        },
        wait_for_rows: (count: number, timeout_ms: number = 5000): Promise<void> => {
            return new Promise((resolve, reject) => {
                if (calls.flat().length >= count) { resolve(); return; }
                const timeout = setTimeout(() => {
                    pending_resolve = null;
                    reject(new Error(`Timed out waiting for ${count} rows (got ${calls.flat().length}) after ${timeout_ms}ms`));
                }, timeout_ms);
                const check = () => {
                    if (calls.flat().length >= count) {
                        clearTimeout(timeout);
                        pending_resolve = null;
                        resolve();
                    } else {
                        pending_resolve = check;
                    }
                };
                pending_resolve = check;
            });
        }
    };
}
