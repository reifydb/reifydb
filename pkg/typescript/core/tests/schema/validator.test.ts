// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {validateSchema} from '../../src/schema/validator';
import {SchemaNode} from '../../src/schema';
import {
    BooleanValue, Int4Value, Float8Value, Utf8Value,
    DateValue, DateTimeValue, TimeValue, DurationValue,
    Uuid4Value, Uuid7Value, BlobValue, NoneValue, DecimalValue,
} from '../../src/value';

describe('validateSchema', () => {
    describe('primitive kind — correct types', () => {
        it('should validate Boolean with boolean value', () => {
            expect(validateSchema({kind: 'primitive', type: 'Boolean'}, true)).toBe(true);
            expect(validateSchema({kind: 'primitive', type: 'Boolean'}, false)).toBe(true);
        });

        it('should validate numeric types with number value', () => {
            const numericTypes = ['Float4', 'Float8', 'Int1', 'Int2', 'Int4'];
            for (const type of numericTypes) {
                expect(validateSchema({kind: 'primitive', type}, 42)).toBe(true);
            }
        });

        it('should validate bigint types with bigint or number', () => {
            const bigintTypes = ['Int8', 'Int16', 'Uint8', 'Uint16'];
            for (const type of bigintTypes) {
                expect(validateSchema({kind: 'primitive', type}, BigInt(42))).toBe(true);
                expect(validateSchema({kind: 'primitive', type}, 42)).toBe(true);
            }
        });

        it('should validate unsigned types with non-negative number', () => {
            const unsignedTypes = ['Uint1', 'Uint2', 'Uint4'];
            for (const type of unsignedTypes) {
                expect(validateSchema({kind: 'primitive', type}, 42)).toBe(true);
            }
        });

        it('should validate string types with string value', () => {
            const stringTypes = ['Utf8', 'Time', 'Duration', 'Uuid4', 'Uuid7'];
            for (const type of stringTypes) {
                expect(validateSchema({kind: 'primitive', type}, 'test')).toBe(true);
            }
        });

        it('should validate Date with Date instance or string', () => {
            expect(validateSchema({kind: 'primitive', type: 'Date'}, new Date())).toBe(true);
            expect(validateSchema({kind: 'primitive', type: 'Date'}, '2024-03-15')).toBe(true);
        });

        it('should validate DateTime with Date instance or string', () => {
            expect(validateSchema({kind: 'primitive', type: 'DateTime'}, new Date())).toBe(true);
            expect(validateSchema({kind: 'primitive', type: 'DateTime'}, '2024-03-15T10:30:00Z')).toBe(true);
        });

        it('should validate Blob with Uint8Array or ArrayBuffer', () => {
            expect(validateSchema({kind: 'primitive', type: 'Blob'}, new Uint8Array([1, 2]))).toBe(true);
            expect(validateSchema({kind: 'primitive', type: 'Blob'}, new ArrayBuffer(4))).toBe(true);
        });

        it('should validate None with undefined', () => {
            expect(validateSchema({kind: 'primitive', type: 'None'}, undefined)).toBe(true);
        });

        it('should validate None with null', () => {
            expect(validateSchema({kind: 'primitive', type: 'None'}, null)).toBe(true);
        });
    });

    describe('primitive kind — wrong types', () => {
        it('should reject Boolean with non-boolean', () => {
            expect(validateSchema({kind: 'primitive', type: 'Boolean'}, 42)).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Boolean'}, 'true')).toBe(false);
        });

        it('should reject Int4 with non-number', () => {
            expect(validateSchema({kind: 'primitive', type: 'Int4'}, 'hello')).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Int4'}, true)).toBe(false);
        });

        it('should reject Utf8 with non-string', () => {
            expect(validateSchema({kind: 'primitive', type: 'Utf8'}, 42)).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Utf8'}, true)).toBe(false);
        });

        it('should reject Blob with non-Uint8Array/ArrayBuffer', () => {
            expect(validateSchema({kind: 'primitive', type: 'Blob'}, 'binary')).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Blob'}, [1, 2, 3])).toBe(false);
        });

        it('should reject unsigned types with negative values', () => {
            expect(validateSchema({kind: 'primitive', type: 'Uint1'}, -1)).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Uint2'}, -100)).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'Uint4'}, -1)).toBe(false);
        });

        it('should reject None with any non-null/undefined value', () => {
            expect(validateSchema({kind: 'primitive', type: 'None'}, 42)).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'None'}, 'hello')).toBe(false);
            expect(validateSchema({kind: 'primitive', type: 'None'}, false)).toBe(false);
        });
    });

    describe('value kind', () => {
        it('should validate Value instance with matching type', () => {
            expect(validateSchema({kind: 'value', type: 'Int4'}, new Int4Value(42))).toBe(true);
            expect(validateSchema({kind: 'value', type: 'Boolean'}, new BooleanValue(true))).toBe(true);
            expect(validateSchema({kind: 'value', type: 'Utf8'}, new Utf8Value('hello'))).toBe(true);
            expect(validateSchema({kind: 'value', type: 'Float8'}, new Float8Value(3.14))).toBe(true);
        });

        it('should reject Value instance with wrong type', () => {
            expect(validateSchema({kind: 'value', type: 'Int4'}, new Utf8Value('hello'))).toBe(false);
            expect(validateSchema({kind: 'value', type: 'Boolean'}, new Int4Value(42))).toBe(false);
        });

        it('should reject raw JS values', () => {
            expect(validateSchema({kind: 'value', type: 'Int4'}, 42)).toBe(false);
            expect(validateSchema({kind: 'value', type: 'Boolean'}, true)).toBe(false);
            expect(validateSchema({kind: 'value', type: 'Utf8'}, 'hello')).toBe(false);
        });

        it('should validate None value kind with null/undefined', () => {
            expect(validateSchema({kind: 'value', type: 'None'}, null)).toBe(true);
            expect(validateSchema({kind: 'value', type: 'None'}, undefined)).toBe(true);
        });

        it('should reject null/undefined for non-None value kind', () => {
            expect(validateSchema({kind: 'value', type: 'Int4'}, null)).toBe(false);
            expect(validateSchema({kind: 'value', type: 'Int4'}, undefined)).toBe(false);
        });

        it('should validate NoneValue instance for None value kind', () => {
            expect(validateSchema({kind: 'value', type: 'None'}, new NoneValue())).toBe(true);
        });
    });

    describe('optional kind', () => {
        it('should accept undefined', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, undefined)).toBe(true);
        });

        it('should accept correct inner value', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, 42)).toBe(true);
        });

        it('should reject wrong inner type', () => {
            const schema: SchemaNode = {kind: 'optional', schema: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, 'hello')).toBe(false);
        });
    });

    describe('object kind', () => {
        it('should validate object with correct fields', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    age: {kind: 'primitive', type: 'Int4'},
                }
            };
            expect(validateSchema(schema, {name: 'Alice', age: 30})).toBe(true);
        });

        it('should reject object with wrong field types', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    age: {kind: 'primitive', type: 'Int4'},
                }
            };
            expect(validateSchema(schema, {name: 'Alice', age: 'not a number'})).toBe(false);
        });

        it('should validate object with optional fields', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {
                    name: {kind: 'primitive', type: 'Utf8'},
                    nickname: {kind: 'optional', schema: {kind: 'primitive', type: 'Utf8'}},
                }
            };
            expect(validateSchema(schema, {name: 'Alice', nickname: undefined})).toBe(true);
            expect(validateSchema(schema, {name: 'Alice', nickname: 'Ali'})).toBe(true);
        });

        it('should reject null', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(validateSchema(schema, null)).toBe(false);
        });

        it('should reject non-object', () => {
            const schema: SchemaNode = {
                kind: 'object',
                properties: {name: {kind: 'primitive', type: 'Utf8'}}
            };
            expect(validateSchema(schema, 'not an object')).toBe(false);
        });
    });

    describe('array kind', () => {
        it('should validate array with correct item types', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, [1, 2, 3])).toBe(true);
        });

        it('should reject array with wrong item types', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, [1, 'two', 3])).toBe(false);
        });

        it('should reject non-array', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, 'not an array')).toBe(false);
        });

        it('should validate empty array', () => {
            const schema: SchemaNode = {kind: 'array', items: {kind: 'primitive', type: 'Int4'}};
            expect(validateSchema(schema, [])).toBe(true);
        });
    });

    describe('unknown kind', () => {
        it('should return false for unknown schema kind', () => {
            const schema = {kind: 'unknown'} as any;
            expect(validateSchema(schema, 42)).toBe(false);
        });
    });
});
