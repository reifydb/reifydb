// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export const WIRE_PASSTHROUGH_KEYS = ['body', 'params', 'credentials'] as const;

function snakeToCamel(key: string): string {
    return key.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
}

function camelToSnake(key: string): string {
    return key.replace(/[A-Z]/g, c => `_${c.toLowerCase()}`);
}

function convertKeys(value: any, convertKey: (key: string) => string, skipKeys: ReadonlySet<string>): any {
    if (Array.isArray(value)) {
        return value.map(item => convertKeys(item, convertKey, skipKeys));
    }
    if (value !== null && typeof value === 'object' && !(value instanceof Date) && !(value instanceof Uint8Array)) {
        const result: Record<string, any> = {};
        for (const [key, val] of Object.entries(value)) {
            result[convertKey(key)] = skipKeys.has(key) ? val : convertKeys(val, convertKey, skipKeys);
        }
        return result;
    }
    return value;
}

export function toCamelCaseKeys<T = any>(value: any, skipKeys: readonly string[] = []): T {
    return convertKeys(value, snakeToCamel, new Set(skipKeys));
}

export function toSnakeCaseKeys<T = any>(value: any, skipKeys: readonly string[] = []): T {
    return convertKeys(value, camelToSnake, new Set(skipKeys));
}
