// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {encodeParams} from '@reifydb/core';
import type {ShapeNode} from '@reifydb/core';

function stable(value: unknown): string {
    if (Array.isArray(value)) {
        return '[' + value.map(stable).join(',') + ']';
    }
    if (value !== null && typeof value === 'object') {
        const record = value as Record<string, unknown>;
        const fields = Object.keys(record).sort().map(k => JSON.stringify(k) + ':' + stable(record[k]));
        return '{' + fields.join(',') + '}';
    }
    return JSON.stringify(value);
}

export function entryKey(rql: string, params: any, shape: ShapeNode): string {
    return rql + '\n' + stable(encodeParams(params)) + '\n' + stable(shape);
}
