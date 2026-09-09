// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {NONE_VALUE, noneMarker, noneMarkerDepth} from "./constant";
import {NoneValue, TypeValuePair, WireType, isOption, typeToWire} from "./value";

export function encodeValue(value: any, name: string = '$1'): TypeValuePair {

    if (value === null || value === undefined) {
        throw new Error(`parameter ${name} is null or undefined, use Option.none(inner)`);
    }

    if (value instanceof NoneValue && value.innerType === 'None') {
        throw new Error(`parameter ${name} is a NoneValue without an inner type, use Option.none(inner)`);
    }

    if (isOption(value)) {
        if (value.isNone()) {
            return { type: value.type, value: NONE_VALUE };
        }
        const inner = encodeValue(value.unwrap(), name);
        const noneAt = noneMarkerDepth(inner.value);
        return {
            type: { Option: inner.type },
            value: noneAt === undefined ? inner.value : noneMarker(noneAt + 1),
        };
    }

    if (value && typeof value === 'object' && 'encode' in value && typeof value.encode === 'function') {
        return value.encode();
    }

    if (typeof value === 'boolean') {
        return { type: 'Boolean', value: value.toString() };
    }

    if (typeof value === 'number') {
        if (Number.isInteger(value)) {
            // Choose appropriate integer type based on value range
            if (value >= -128 && value <= 127) {
                return { type: 'Int1', value: value.toString() };
            } else if (value >= -32768 && value <= 32767) {
                return { type: 'Int2', value: value.toString() };
            } else if (value >= -2147483648 && value <= 2147483647) {
                return { type: 'Int4', value: value.toString() };
            } else {
                return { type: 'Int8', value: value.toString() };
            }
        } else {
            // Floating point number
            return { type: 'Float8', value: value.toString() };
        }
    }

    if (typeof value === 'string') {
        if (/^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
            const version = value[14];
            if (version === '4') {
                return { type: 'Uuid4', value: value };
            } else if (version === '7') {
                return { type: 'Uuid7', value: value };
            }
        }
        return { type: 'Utf8', value: value };
    }

    if (typeof value === 'bigint') {
        if (value >= BigInt(0)) {
            if (value <= BigInt(255)) {
                return { type: 'Uint1', value: value.toString() };
            } else if (value <= BigInt(65535)) {
                return { type: 'Uint2', value: value.toString() };
            } else if (value <= BigInt(4294967295)) {
                return { type: 'Uint4', value: value.toString() };
            } else if (value <= BigInt('18446744073709551615')) {
                return { type: 'Uint8', value: value.toString() };
            } else {
                return { type: 'Uint16', value: value.toString() };
            }
        } else {
            if (value >= BigInt('-9223372036854775808')) {
                return { type: 'Int8', value: value.toString() };
            } else {
                return { type: 'Int16', value: value.toString() };
            }
        }
    }

    if (value instanceof Date) {
        return { type: 'DateTime', value: value.toISOString() };
    }


    if (value instanceof Uint8Array) {
        const hex = Array.from(value)
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
        return { type: 'Blob', value: '0x' + hex };
    }

    if (value instanceof ArrayBuffer) {
        const uint8Array = new Uint8Array(value);
        const hex = Array.from(uint8Array)
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
        return { type: 'Blob', value: '0x' + hex };
    }

    throw new Error(`Cannot encode value of type ${typeof value}: ${value}`);
}

/** A parameter as it goes on the wire: the same value, with the type in the wire's own rendering. */
export interface WireValue {
    type: WireType;
    value: string;
}

function toWire(pair: TypeValuePair): WireValue {
    return {type: typeToWire(pair.type), value: pair.value};
}

export function encodeParams(params: any): WireValue[] | Record<string, WireValue> {
    if (params === undefined || params === null) {
        return [];
    }

    if (Array.isArray(params)) {
        return params.map((param, i) => toWire(encodeValue(param, `$${i + 1}`)));
    } else if (typeof params === 'object') {
        const encoded: Record<string, WireValue> = {};
        for (const [key, value] of Object.entries(params)) {
            encoded[key] = toWire(encodeValue(value, `$${key}`));
        }
        return encoded;
    }

    throw new Error(`Invalid parameters type: expected array or object, got ${typeof params}`);
}
