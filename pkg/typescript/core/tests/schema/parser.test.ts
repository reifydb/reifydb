// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {parseValue} from '../../src/schema/parser';
import {SchemaNode} from '../../src/schema';
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
                const schema: SchemaNode = {kind: 'primitive', type};
                const result = parseValue(schema, value);
                expect(result).toBeInstanceOf(expectedClass);
            });
        });

        it('should return undefined for None type with undefined value', () => {
            const schema: SchemaNode = {kind: 'primitive', type: 'None'};
            expect(parseValue(schema, undefined)).toBeUndefined();
        });

        it('should parse None type with non-null value', () => {
            const schema: SchemaNode = {kind: 'primitive', type: 'None'};
            const result = parseValue(schema, 'anything');
            expect(result).toBeInstanceOf(NoneValue);
        });

        it('should return undefined for null value', () => {
            const schema: SchemaNode = {kind: 'primitive', type: 'Int4'};
            expect(parseValue(schema, null)).toBeUndefined();
        });

        it('should return undefined for undefined value', () => {
            const schema: SchemaNode = {kind: 'primitive', type: 'Int4'};
            expect(parseValue(schema, undefined)).toBeUndefined();
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
                const schema: SchemaNode = {kind: 'value', type};
                const result = parseValue(schema, value);
                expect(result).toBeInstanceOf(expectedClass);
            });
        });

        it('should parse value kind None', () => {
            const schema: SchemaNode = {kind: 'value', type: 'None'};
            const result = parseValue(schema, undefined);
            expect(result).toBeUndefined();
        });

        it('should return undefined for null value with value kind', () => {
            const schema: SchemaNode = {kind: 'value', type: 'Int4'};
            expect(parseValue(schema, null)).toBeUndefined();
        });

        it('should return undefined for undefined value with value kind', () => {
            const schema: SchemaNode = {kind: 'value', type: 'Int4'};
            expect(parseValue(schema, undefined)).toBeUndefined();
        });
    });

    describe('optional kind', () => {
        it('should return undefined for undefined value', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(schema, undefined)).toBeUndefined();
        });

        it('should parse inner schema when value is present', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'primitive', type: 'Int4'}};
            const result = parseValue(schema, 42);
            expect(result).toBeInstanceOf(Int4Value);
        });

        it('should handle optional wrapping a value kind', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'value', type: 'Utf8'}};
            const result = parseValue(schema, 'hello');
            expect(result).toBeInstanceOf(Utf8Value);
        });

        it('should return undefined for optional wrapping value kind with undefined', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'value', type: 'Utf8'}};
            expect(parseValue(schema, undefined)).toBeUndefined();
        });
    });

    describe('object kind', () => {
        it('should parse object with primitive fields', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    age: {kind: 'primitive', type: 'Int4'},
                }
            };
            const result = parseValue(schema, {name: 'Alice', age: 30});
            expect(result.name).toBeInstanceOf(Utf8Value);
            expect(result.age).toBeInstanceOf(Int4Value);
        });

        it('should parse object with optional fields', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    nickname: {kind: 'optional', schema: {kind: 'primitive', type: 'Utf8'}},
                }
            };
            const result = parseValue(schema, {name: 'Alice', nickname: undefined});
            expect(result.name).toBeInstanceOf(Utf8Value);
            expect(result.nickname).toBeUndefined();
        });

        it('should return undefined for null object', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(parseValue(schema, null)).toBeUndefined();
        });

        it('should return undefined for undefined object', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(parseValue(schema, undefined)).toBeUndefined();
        });
    });

    describe('array kind', () => {
        it('should parse array of primitives', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            const result = parseValue(schema, [1, 2, 3]);
            expect(result).toHaveLength(3);
            expect(result[0]).toBeInstanceOf(Int4Value);
            expect(result[1]).toBeInstanceOf(Int4Value);
            expect(result[2]).toBeInstanceOf(Int4Value);
        });

        it('should return empty array for non-array input', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(schema, 'not an array')).toEqual([]);
        });

        it('should return empty array for null', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(parseValue(schema, null)).toEqual([]);
        });
    });

    describe('unknown kind', () => {
        it('should throw for unknown schema kind', () => {
            const schema = {kind: 'unknown'} as any;
            expect(() => parseValue(schema, 42)).toThrow('Unknown schema kind: unknown');
        });
    });
});
