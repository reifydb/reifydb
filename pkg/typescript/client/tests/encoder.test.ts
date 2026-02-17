// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {encodeValue, encodeParams} from '../src/encoder';
import {
    NONE_VALUE, NoneValue, Int4Value, BooleanValue, Utf8Value, Float8Value
} from '@reifydb/core';

describe('encodeValue', () => {
    it('should encode null as None', () => {
        const result = encodeValue(null);
        expect(result.type).toBe('None');
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should encode undefined as None', () => {
        const result = encodeValue(undefined);
        expect(result.type).toBe('None');
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should encode NoneValue via .encode()', () => {
        const result = encodeValue(new NoneValue());
        expect(result.type).toBe('None');
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should encode Int4Value(undefined) as Int4 with NONE_VALUE', () => {
        const result = encodeValue(new Int4Value(undefined));
        expect(result.type).toBe('Int4');
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should encode Int4Value with value', () => {
        const result = encodeValue(new Int4Value(42));
        expect(result.type).toBe('Int4');
        expect(result.value).toBe('42');
    });

    it('should encode BooleanValue via .encode()', () => {
        const result = encodeValue(new BooleanValue(true));
        expect(result.type).toBe('Boolean');
        expect(result.value).toBe('true');
    });

    it('should encode BooleanValue(undefined) as Boolean with NONE_VALUE', () => {
        const result = encodeValue(new BooleanValue(undefined));
        expect(result.type).toBe('Boolean');
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should encode raw boolean', () => {
        const result = encodeValue(true);
        expect(result.type).toBe('Boolean');
        expect(result.value).toBe('true');
    });

    it('should encode raw number as integer', () => {
        const result = encodeValue(42);
        expect(result.type).toBe('Int1');
        expect(result.value).toBe('42');
    });

    it('should encode large number as Int4', () => {
        const result = encodeValue(100000);
        expect(result.type).toBe('Int4');
        expect(result.value).toBe('100000');
    });

    it('should encode float as Float8', () => {
        const result = encodeValue(3.14);
        expect(result.type).toBe('Float8');
        expect(result.value).toBe('3.14');
    });

    it('should encode raw string as Utf8', () => {
        const result = encodeValue('hello');
        expect(result.type).toBe('Utf8');
        expect(result.value).toBe('hello');
    });

    it('should encode UUID v4 string as Uuid4', () => {
        const result = encodeValue('550e8400-e29b-41d4-a716-446655440000');
        expect(result.type).toBe('Uuid4');
        expect(result.value).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('should encode Date object as DateTime', () => {
        const date = new Date('2024-03-15T10:30:00Z');
        const result = encodeValue(date);
        expect(result.type).toBe('DateTime');
    });

    it('should encode Uint8Array as Blob', () => {
        const result = encodeValue(new Uint8Array([1, 2, 3]));
        expect(result.type).toBe('Blob');
        expect(result.value).toBe('0x010203');
    });

    it('should encode bigint as appropriate uint type', () => {
        const result = encodeValue(BigInt(42));
        expect(result.type).toBe('Uint1');
        expect(result.value).toBe('42');
    });

    it('should throw for unsupported value type', () => {
        expect(() => encodeValue(Symbol('test') as any)).toThrow();
    });
});

describe('encodeParams', () => {
    it('should encode array of null and undefined as None pairs', () => {
        const result = encodeParams([null, undefined, new NoneValue()]);
        expect(Array.isArray(result)).toBe(true);
        const arr = result as any[];
        expect(arr).toHaveLength(3);
        expect(arr[0].type).toBe('None');
        expect(arr[0].value).toBe(NONE_VALUE);
        expect(arr[1].type).toBe('None');
        expect(arr[1].value).toBe(NONE_VALUE);
        expect(arr[2].type).toBe('None');
        expect(arr[2].value).toBe(NONE_VALUE);
    });

    it('should encode named params with null and undefined', () => {
        const result = encodeParams({a: null, b: undefined});
        expect(typeof result).toBe('object');
        expect(Array.isArray(result)).toBe(false);
        const obj = result as Record<string, any>;
        expect(obj.a.type).toBe('None');
        expect(obj.a.value).toBe(NONE_VALUE);
        expect(obj.b.type).toBe('None');
        expect(obj.b.value).toBe(NONE_VALUE);
    });

    it('should return empty array for null params', () => {
        expect(encodeParams(null)).toEqual([]);
    });

    it('should return empty array for undefined params', () => {
        expect(encodeParams(undefined)).toEqual([]);
    });

    it('should encode mixed array params', () => {
        const result = encodeParams([42, 'hello', true, null]);
        const arr = result as any[];
        expect(arr).toHaveLength(4);
        expect(arr[0].type).toBe('Int1');
        expect(arr[1].type).toBe('Utf8');
        expect(arr[2].type).toBe('Boolean');
        expect(arr[3].type).toBe('None');
    });

    it('should encode mixed named params', () => {
        const result = encodeParams({count: 42, name: 'test', active: true});
        const obj = result as Record<string, any>;
        expect(obj.count.type).toBe('Int1');
        expect(obj.name.type).toBe('Utf8');
        expect(obj.active.type).toBe('Boolean');
    });

    it('should throw for invalid params type', () => {
        expect(() => encodeParams('invalid' as any)).toThrow('Invalid parameters type');
    });
});
