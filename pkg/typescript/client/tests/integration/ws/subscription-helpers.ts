// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import type { WsClient } from "../../../src";
import type { SchemaNode, InferSchema } from '@reifydb/core';

/**
 * Create a unique test table name to avoid conflicts
 */
export function createTestTableName(prefix: string = 'test'): string {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Helper to create a test table with schema
 * Creates a 'test' namespace if it doesn't exist and uses test.table_name
 */
export async function createTestTable(
    client: WsClient,
    tableName: string,
    columns: string[]
): Promise<void> {
    // Ensure namespace exists
    try {
        await client.command(`create namespace test`, null, []);
    } catch (err) {
        // Ignore if already exists
    }

    const columnDefs = columns.map(col => {
        const [name, type] = col.split(' ');
        return `${name}: ${type.toLowerCase()}`;
    }).join(', ');

    await client.command(
        `create table test.${tableName} { ${columnDefs} }`,
        null,
        []
    );
}

/**
 * Wait for a subscription callback to be invoked with timeout
 */
// Overload 1: With schema (type inferred)
// @ts-ignore
export function waitForCallback<S extends SchemaNode>(
    schema: S,
    timeoutMs?: number
): {
    promise: Promise<InferSchema<S>[]>,
    callback: (rows: InferSchema<S>[]) => void
};

// Overload 2: Without schema (explicit type)
export function waitForCallback<T = any>(
    timeoutMs?: number
): {
    promise: Promise<T[]>,
    callback: (rows: T[]) => void
};

// Implementation
export function waitForCallback<S extends SchemaNode = any>(
    schemaOrTimeout?: S | number,
    timeoutMs: number = 500
): {
    promise: Promise<any[]>,
    callback: (rows: any[]) => void
} {
    // Handle overload parameters
    const timeout = typeof schemaOrTimeout === 'number' ? schemaOrTimeout : timeoutMs;

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
// Overload 1: With schema (type inferred)
export function createCallbackTracker<S extends SchemaNode>(
    schema: S
): {
    callback: (rows: InferSchema<S>[]) => void;
    getCalls: () => InferSchema<S>[][];
    getCallCount: () => number;
    getAllRows: () => InferSchema<S>[];
    clear: () => void;
};

// Overload 2: Without schema (explicit type)
export function createCallbackTracker<T = any>(): {
    callback: (rows: T[]) => void;
    getCalls: () => T[][];
    getCallCount: () => number;
    getAllRows: () => T[];
    clear: () => void;
};

// Implementation
export function createCallbackTracker<S extends SchemaNode = any>(
    schema?: S
) {
    const calls: any[][] = [];

    return {
        callback: (rows: any[]) => {
            calls.push(rows);
        },
        getCalls: () => calls,
        getCallCount: () => calls.length,
        getAllRows: () => calls.flat(),
        clear: () => {
            calls.length = 0;
        }
    };
}
