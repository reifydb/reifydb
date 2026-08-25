// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export type ValueRole =
    | 'none'
    | 'boolean'
    | 'number'
    | 'temporal'
    | 'uuid'
    | 'string'
    | 'blob'
    | 'unknown';

const ROLE_BY_TYPE: Record<string, ValueRole> = {
    None: 'none',
    Boolean: 'boolean',
    Int1: 'number',
    Int2: 'number',
    Int4: 'number',
    Int8: 'number',
    Int16: 'number',
    Uint1: 'number',
    Uint2: 'number',
    Uint4: 'number',
    Uint8: 'number',
    Uint16: 'number',
    Float4: 'number',
    Float8: 'number',
    Decimal: 'number',
    Date: 'temporal',
    DateTime: 'temporal',
    Time: 'temporal',
    Duration: 'temporal',
    Uuid4: 'uuid',
    Uuid7: 'uuid',
    IdentityId: 'uuid',
    Utf8: 'string',
    Blob: 'blob',
};

function type_name(value: unknown): string | undefined {
    if (value === null || typeof value !== 'object') return undefined;
    const candidate = (value as {type?: unknown}).type;
    return typeof candidate === 'string' ? candidate : undefined;
}

export function value_type_name(value: unknown): string | undefined {
    if (value === null || value === undefined) return 'None';
    return type_name(value);
}

export function value_role(value: unknown): ValueRole {
    if (value === null || value === undefined) return 'none';

    const name = type_name(value);
    if (name !== undefined) {
        return ROLE_BY_TYPE[name] ?? 'unknown';
    }

    switch (typeof value) {
        case 'number':
        case 'bigint':
            return 'number';
        case 'boolean':
            return 'boolean';
        case 'string':
            return 'string';
        default:
            return 'unknown';
    }
}

export function format_value(value: unknown): string {
    if (value === null || value === undefined) return 'none';
    if (typeof value === 'bigint') return value.toString();

    if (typeof value === 'object') {
        if (type_name(value) !== undefined) return String(value);
        return JSON.stringify(value) ?? String(value);
    }

    return String(value);
}
