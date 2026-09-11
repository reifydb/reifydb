// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {decode} from '../decoder';
import {Type, Value, isOptionType} from '../value';

// A value shape names the type the caller wants to hold, so it has to accept a column whose type the
// requested one can represent in full. The server picks the narrowest type a literal fits, so
// `MAP {n: 1}` arrives as Int1 and a caller asking for an Int4Value would otherwise be refused a
// value that fits with room to spare. A primitive shape has always tolerated the whole numeric
// family and coerced into it; this is the same tolerance for value shapes.
//
// Only widening is allowed. A narrower target would silently drop range, a float target would claim
// a precision the integer never had, and a different family would change what the value means.

interface Numeric {
    kind: 'int' | 'uint' | 'float';
    bits: number;
}

const NUMERIC: Readonly<Record<string, Numeric>> = {
    Int1: {kind: 'int', bits: 8},
    Int2: {kind: 'int', bits: 16},
    Int4: {kind: 'int', bits: 32},
    Int8: {kind: 'int', bits: 64},
    Int16: {kind: 'int', bits: 128},
    Uint1: {kind: 'uint', bits: 8},
    Uint2: {kind: 'uint', bits: 16},
    Uint4: {kind: 'uint', bits: 32},
    Uint8: {kind: 'uint', bits: 64},
    Uint16: {kind: 'uint', bits: 128},
    Float4: {kind: 'float', bits: 32},
    Float8: {kind: 'float', bits: 64},
};

/** Whether every value of `actual` is representable as `target`, the type a value shape names. */
export function widens(target: string, actual: Type): boolean {
    if (isOptionType(actual)) {
        return false;
    }
    const t = NUMERIC[target];
    const a = NUMERIC[actual];
    if (!t || !a) {
        return target === actual;
    }
    if (t.kind === 'float' || a.kind === 'float') {
        return t.kind === 'float' && a.kind === 'float' && t.bits >= a.bits;
    }
    if (t.kind === 'uint') {
        return a.kind === 'uint' && t.bits >= a.bits;
    }
    // A signed target holds an unsigned source only when it is strictly wider: Uint1 reaches 255,
    // which an Int1 cannot hold, but an Int2 can.
    return a.kind === 'int' ? t.bits >= a.bits : t.bits > a.bits;
}

/** Re-reads `value` as `target` when the target is wider; otherwise hands it back untouched. */
export function widen(value: any, target: string): any {
    if (!value || typeof value !== 'object' || typeof (value as Value).type !== 'string') {
        return value;
    }
    const actual = (value as Value).type;
    if (actual === target || !widens(target, actual)) {
        return value;
    }
    const inner = (value as any).value;
    if (inner === undefined || inner === null) {
        return value;
    }
    return decode({type: target as Type, value: String(inner)});
}
