// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {encodeValue, encodeParams} from '../src/encoder';
import {
    NONE_VALUE, NoneValue, Int4Value, BooleanValue, Utf8Value, Float8Value, Option, noneMarker
} from '@reifydb/core';

describe('encodeValue', () => {
    it('should reject null naming the parameter', () => {
        expect(() => encodeValue(null)).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
    });

    it('should reject undefined naming the parameter', () => {
        expect(() => encodeValue(undefined)).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
    });

    it('should encode a typed NoneValue via .encode() as the Option of its inner type', () => {
        const result = encodeValue(new NoneValue('Int4'));
        expect(result.type).toEqual({Option: 'Int4'});
        expect(result.value).toBe(NONE_VALUE);
    });

    it('should reject a NoneValue without an inner type naming the parameter', () => {
        expect(() => encodeValue(new NoneValue())).toThrow('parameter $1 is a NoneValue without an inner type, use Option.none(inner)');
    });

    it('should encode Option.some over base types with the type wrapped in one Option layer', () => {
        expect(encodeValue(Option.some(new Int4Value(42)))).toEqual({type: {Option: 'Int4'}, value: '42'});
        expect(encodeValue(Option.some(new Utf8Value('hello')))).toEqual({type: {Option: 'Utf8'}, value: 'hello'});
        expect(encodeValue(Option.some(new BooleanValue(true)))).toEqual({type: {Option: 'Boolean'}, value: 'true'});
        expect(encodeValue(Option.some(new Float8Value(3.14)))).toEqual({type: {Option: 'Float8'}, value: '3.14'});
    });

    it('should encode Option.none as the Option of the inner type with the bare none marker', () => {
        expect(encodeValue(Option.none('Int4'))).toEqual({type: {Option: 'Int4'}, value: NONE_VALUE});
        expect(encodeValue(Option.none('Utf8'))).toEqual({type: {Option: 'Utf8'}, value: NONE_VALUE});
    });

    it('should encode nested Options with one Option layer per level and the none marker at its depth', () => {
        expect(encodeValue(Option.some(Option.some(new Int4Value(1))))).toEqual({type: {Option: {Option: 'Int4'}}, value: '1'});
        expect(encodeValue(Option.some(Option.none('Int4')))).toEqual({type: {Option: {Option: 'Int4'}}, value: noneMarker(1)});
        expect(encodeValue(Option.none({Option: 'Int4'}))).toEqual({type: {Option: {Option: 'Int4'}}, value: NONE_VALUE});
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
    it('should reject null and undefined in an array naming the position', () => {
        expect(() => encodeParams([42, null])).toThrow('parameter $2 is null or undefined, use Option.none(inner)');
        expect(() => encodeParams([undefined])).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
        expect(() => encodeParams([42, 'hello', new NoneValue()])).toThrow('parameter $3 is a NoneValue without an inner type, use Option.none(inner)');
    });

    it('should reject null and undefined in named params naming the parameter', () => {
        expect(() => encodeParams({name: null})).toThrow('parameter $name is null or undefined, use Option.none(inner)');
        expect(() => encodeParams({a: 1, name: undefined})).toThrow('parameter $name is null or undefined, use Option.none(inner)');
    });

    it('should encode positional Option params', () => {
        expect(encodeParams([Option.some(new Int4Value(42)), Option.none('Utf8'), Option.some(Option.none('Int4')), Option.none({Option: 'Int4'})])).toEqual([
            {type: {Option: 'Int4'}, value: '42'},
            {type: {Option: 'Utf8'}, value: NONE_VALUE},
            {type: {Option: {Option: 'Int4'}}, value: noneMarker(1)},
            {type: {Option: {Option: 'Int4'}}, value: NONE_VALUE},
        ]);
    });

    it('should encode named Option params', () => {
        expect(encodeParams({
            count: Option.some(new Int4Value(42)),
            name: Option.none('Utf8'),
            inner: Option.some(Option.some(new Int4Value(1))),
            outer: Option.none({Option: 'Int4'}),
        })).toEqual({
            count: {type: {Option: 'Int4'}, value: '42'},
            name: {type: {Option: 'Utf8'}, value: NONE_VALUE},
            inner: {type: {Option: {Option: 'Int4'}}, value: '1'},
            outer: {type: {Option: {Option: 'Int4'}}, value: NONE_VALUE},
        });
    });

    it('should return empty array for null params', () => {
        expect(encodeParams(null)).toEqual([]);
    });

    it('should return empty array for undefined params', () => {
        expect(encodeParams(undefined)).toEqual([]);
    });

    it('should encode mixed array params and reject a trailing null naming its position', () => {
        const result = encodeParams([42, 'hello', true]);
        const arr = result as any[];
        expect(arr).toHaveLength(3);
        expect(arr[0].type).toBe('Int1');
        expect(arr[1].type).toBe('Utf8');
        expect(arr[2].type).toBe('Boolean');
        expect(() => encodeParams([42, 'hello', true, null])).toThrow('parameter $4 is null or undefined, use Option.none(inner)');
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
