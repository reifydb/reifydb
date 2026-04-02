// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {parseValue} from '../../src/shape/parser';
import {ShapeNode} from '../../src/shape';
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue,
    Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    DurationValue, TimeValue,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    NoneValue, Utf8Value, Uuid4Value, Uuid7Value, IdentityIdValue
} from '../../src/value';

describe('parseValue', () => {
    describe('primitive kind', () => {
        const primitiveTests: Array<{type: string, value: any, expectedClass: any}> = [
            {type: 'Boolean', value: true, expectedClass: BooleanValue},
            {type: 'Int1', value: 42, expectedClass: Int1Value},
            {type: 'Int2', value: 1000, expectedClass: Int2Value},
            {type: 'Int4', value: 100000, expectedClass: Int4Value},
            {type: 'Int8', value: 123456789, expectedClass: Int8Value},
            {type: 'Int16', value: 12345, expectedClass: Int16Value},
            {type: 'Uint1', value: 42, expectedClass: Uint1Value},
            {type: 'Uint2', value: 1000, expectedClass: Uint2Value},
            {type: 'Uint4', value: 100000, expectedClass: Uint4Value},
            {type: 'Uint8', value: 123456789, expectedClass: Uint8Value},
            {type: 'Uint16', value: 12345, expectedClass: Uint16Value},
            {type: 'Float4', value: 3.14, expectedClass: Float4Value},
            {type: 'Float8', value: 3.141592653589793, expectedClass: Float8Value},
            {type: 'Utf8', value: 'hello', expectedClass: Utf8Value},
            {type: 'Date', value: '2024-03-15', expectedClass: DateValue},
            {type: 'DateTime', value: '2024-03-15T10:30:00Z', expectedClass: DateTimeValue},
            {type: 'Time', value: '10:30:00', expectedClass: TimeValue},
            {type: 'Duration', value: 'PT1H', expectedClass: DurationValue},
            {type: 'Uuid4', value: '550e8400-e29b-41d4-a716-446655440000', expectedClass: Uuid4Value},
            {type: 'Uuid7', value: '01932c07-a000-7000-8000-000000000000', expectedClass: Uuid7Value},
            {type: 'Decimal', value: '123.456', expectedClass: DecimalValue},
        ];

        primitiveTests.forEach(({type, value, expectedClass}) => {
            it(`should parse ${type} with actual value`, () => {
                const shape: ShapeNode = {kind: 'primitive', type};
                const result = parseValue(shape, value);
                expect(result).toBeInstanceOf(expectedClass);
            });
        });

        it('should return undefined for None type with undefined value', () => {
            const shape: ShapeNode = {kind: 'primitive', type: 'None'};
            expect(parseValue(shape, undefined)).toBeUndefined();
        });

        it('should parse None type with non-null value', () => {
            const shape: ShapeNode = {kind: 'primitive', type: 'None'};
            const result = parseValue(shape, 'anything');
            expect(result).toBeInstanceOf(NoneValue);
        });

        it('should return undefined for null value', () => {
            const shape: ShapeNode = {kind: 'primitive', type: 'Int4'};
            expect(parseValue(shape, null)).toBeUndefined();
        });

        it('should return undefined for undefined value', () => {
            const shape: ShapeNode = {kind: 'primitive', type: 'Int4'};
            expect(parseValue(shape, undefined)).toBeUndefined();
        });
    });

    describe('value kind', () => {
        const valueTests: Array<{type: string, value: any, expectedClass: any}> = [
            {type: 'Boolean', value: true, expectedClass: BooleanValue},
            {type: 'Int1', value: 42, expectedClass: Int1Value},
            {type: 'Int2', value: 1000, expectedClass: Int2Value},
            {type: 'Int4', value: 100000, expectedClass: Int4Value},
            {type: 'Int8', value: 123456789, expectedClass: Int8Value},
            {type: 'Int16', value: 12345, expectedClass: Int16Value},
            {type: 'Uint1', value: 42, expectedClass: Uint1Value},
            {type: 'Uint2', value: 1000, expectedClass: Uint2Value},
            {type: 'Uint4', value: 100000, expectedClass: Uint4Value},
            {type: 'Uint8', value: 123456789, expectedClass: Uint8Value},
            {type: 'Uint16', value: 12345, expectedClass: Uint16Value},
            {type: 'Float4', value: 3.14, expectedClass: Float4Value},
            {type: 'Float8', value: 3.141592653589793, expectedClass: Float8Value},
            {type: 'Utf8', value: 'hello', expectedClass: Utf8Value},
            {type: 'Date', value: '2024-03-15', expectedClass: DateValue},
            {type: 'DateTime', value: '2024-03-15T10:30:00Z', expectedClass: DateTimeValue},
            {type: 'Time', value: '10:30:00', expectedClass: TimeValue},
            {type: 'Duration', value: 'PT1H', expectedClass: DurationValue},
            {type: 'Uuid4', value: '550e8400-e29b-41d4-a716-446655440000', expectedClass: Uuid4Value},
            {type: 'Uuid7', value: '01932c07-a000-7000-8000-000000000000', expectedClass: Uuid7Value},
            {type: 'Decimal', value: '123.456', expectedClass: DecimalValue},
        ];

        valueTests.forEach(({type, value, expectedClass}) => {
            it(`should parse value kind ${type} with actual value`, () => {
                const shape: ShapeNode = {kind: 'value', type};
                const result = parseValue(shape, value);
                expect(result).toBeInstanceOf(expectedClass);
            });
        });

        it('should parse value kind None', () => {
            const shape: ShapeNode = {kind: 'value', type: 'None'};
            const result = parseValue(shape, undefined);
            expect(result).toBeUndefined();
        });

        it('should return undefined for null value with value kind', () => {
            const shape: ShapeNode = {kind: 'value', type: 'Int4'};
            expect(parseValue(shape, null)).toBeUndefined();
        });

        it('should return undefined for undefined value with value kind', () => {
            const shape: ShapeNode = {kind: 'value', type: 'Int4'};
            expect(parseValue(shape, undefined)).toBeUndefined();
        });
    });

    describe('optional kind', () => {
        it('should return undefined for undefined value', () => {
            const shape: ShapeNode = {kind: 'optional', shape: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(shape, undefined)).toBeUndefined();
        });

        it('should parse inner shape when value is present', () => {
            const shape: ShapeNode = {kind: 'optional', shape: {kind: 'primitive', type: 'Int4'}};
            const result = parseValue(shape, 42);
            expect(result).toBeInstanceOf(Int4Value);
        });

        it('should handle optional wrapping a value kind', () => {
            const shape: ShapeNode = {kind: 'optional', shape: {kind: 'value', type: 'Utf8'}};
            const result = parseValue(shape, 'hello');
            expect(result).toBeInstanceOf(Utf8Value);
        });

        it('should return undefined for optional wrapping value kind with undefined', () => {
            const shape: ShapeNode = {kind: 'optional', shape: {kind: 'value', type: 'Utf8'}};
            expect(parseValue(shape, undefined)).toBeUndefined();
        });
    });

    describe('object kind', () => {
        it('should parse object with primitive fields', () => {
            const shape: ShapeNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    age: {kind: 'primitive', type: 'Int4'},
                }
            };
            const result = parseValue(shape, {name: 'Alice', age: 30});
            expect(result.name).toBeInstanceOf(Utf8Value);
            expect(result.age).toBeInstanceOf(Int4Value);
        });

        it('should parse object with optional fields', () => {
            const shape: ShapeNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    nickname: {kind: 'optional', shape: {kind: 'primitive', type: 'Utf8'}},
                }
            };
            const result = parseValue(shape, {name: 'Alice', nickname: undefined});
            expect(result.name).toBeInstanceOf(Utf8Value);
            expect(result.nickname).toBeUndefined();
        });

        it('should return undefined for null object', () => {
            const shape: ShapeNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(parseValue(shape, null)).toBeUndefined();
        });

        it('should return undefined for undefined object', () => {
            const shape: ShapeNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(parseValue(shape, undefined)).toBeUndefined();
        });
    });

    describe('array kind', () => {
        it('should parse array of primitives', () => {
            const shape: ShapeNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            const result = parseValue(shape, [1, 2, 3]);
            expect(result).toHaveLength(3);
            expect(result[0]).toBeInstanceOf(Int4Value);
            expect(result[1]).toBeInstanceOf(Int4Value);
            expect(result[2]).toBeInstanceOf(Int4Value);
        });

        it('should return empty array for non-array input', () => {
            const shape: ShapeNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(shape, 'not an array')).toEqual([]);
        });

        it('should return empty array for null', () => {
            const shape: ShapeNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(shape, null)).toEqual([]);
        });
    });

    describe('unknown kind', () => {
        it('should throw for unknown shape kind', () => {
            const shape = {kind: 'unknown'} as any;
            expect(() => parseValue(shape, 42)).toThrow('Unknown shape kind: unknown');
        });
    });
});
