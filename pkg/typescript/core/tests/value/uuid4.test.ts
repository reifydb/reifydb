// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { Uuid4Value } from '../../src';
import { NIL as NIL_UUID } from 'uuid';

describe('Uuid4Value', () => {
    describe('constructor', () => {
        it('should create instance with valid UUID v4', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = new Uuid4Value(uuid);
            expect(value.asString()).toBe(uuid);
        });

        it('should create instance with nil UUID', () => {
            const value = new Uuid4Value(NIL_UUID);
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });

        it('should create instance with undefined', () => {
            const value = new Uuid4Value(undefined);
            expect(value.asString()).toBeUndefined();
        });

        it('should convert to lowercase', () => {
            const uuid = '550E8400-E29B-41D4-A716-446655440000';
            const value = new Uuid4Value(uuid);
            expect(value.asString()).toBe(uuid.toLowerCase());
        });

        it('should throw error for invalid UUID format', () => {
            expect(() => new Uuid4Value('not-a-uuid')).toThrow('Invalid UUID format');
            expect(() => new Uuid4Value('550e8400-e29b-41d4-a716')).toThrow('Invalid UUID format');
        });

        it('should throw error for wrong UUID version', () => {
            // This is a UUID v1
            const uuidV1 = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
            expect(() => new Uuid4Value(uuidV1)).toThrow('Invalid UUID version for Uuid4');
        });

        it('should throw error for non-string value', () => {
            expect(() => new Uuid4Value(123 as any)).toThrow('Uuid4 value must be a string');
        });
    });

    describe('generate', () => {
        it('should generate a new UUID v4', () => {
            const value = Uuid4Value.generate();
            expect(value.asString()).toBeDefined();
            expect(value.getVersion()).toBe(4);
        });

        it('should generate unique UUIDs', () => {
            const value1 = Uuid4Value.generate();
            const value2 = Uuid4Value.generate();
            expect(value1.asString()).not.toBe(value2.asString());
        });
    });

    describe('nil', () => {
        it('should create nil UUID', () => {
            const value = Uuid4Value.nil();
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });
    });

    describe('default', () => {
        it('should return nil UUID as default', () => {
            const value = Uuid4Value.default();
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });
    });

    describe('parse', () => {
        it('should parse valid UUID v4 string', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = Uuid4Value.parse(uuid);
            expect(value.asString()).toBe(uuid);
        });

        it('should parse nil UUID', () => {
            const value = Uuid4Value.parse(NIL_UUID);
            expect(value.asString()).toBe(NIL_UUID);
        });

        it('should parse with whitespace', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = Uuid4Value.parse(`  ${uuid}  `);
            expect(value.asString()).toBe(uuid);
        });

        it('should return undefined for empty string', () => {
            expect(Uuid4Value.parse('').value).toBeUndefined();
            expect(Uuid4Value.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for NONE_VALUE', () => {
            expect(Uuid4Value.parse('⟪none⟫').value).toBeUndefined();
        });

        it('should throw error for invalid UUID', () => {
            expect(() => Uuid4Value.parse('not-a-uuid')).toThrow('Cannot parse');
        });

        it('should throw error for wrong version', () => {
            // UUID v1
            const uuidV1 = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
            expect(() => Uuid4Value.parse(uuidV1)).toThrow('wrong version');
        });
    });

    describe('asBytes', () => {
        it('should convert UUID to 16-byte array', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = new Uuid4Value(uuid);
            const bytes = value.asBytes();
            
            expect(bytes).toBeInstanceOf(Uint8Array);
            expect(bytes?.length).toBe(16);
            expect(bytes?.[0]).toBe(0x55);
            expect(bytes?.[1]).toBe(0x0e);
            expect(bytes?.[2]).toBe(0x84);
            expect(bytes?.[3]).toBe(0x00);
        });

        it('should return undefined for undefined UUID', () => {
            const value = new Uuid4Value(undefined);
            expect(value.asBytes()).toBeUndefined();
        });
    });

    describe('getVersion', () => {
        it('should return version 4 for v4 UUID', () => {
            const value = Uuid4Value.generate();
            expect(value.getVersion()).toBe(4);
        });

        it('should return 0 for nil UUID', () => {
            const value = Uuid4Value.nil();
            expect(value.getVersion()).toBe(0);
        });

        it('should return undefined for undefined UUID', () => {
            const value = new Uuid4Value(undefined);
            expect(value.getVersion()).toBeUndefined();
        });
    });

    describe('toString', () => {
        it('should format UUID as string', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = new Uuid4Value(uuid);
            expect(value.toString()).toBe(uuid);
        });

        it('should format undefined as "undefined"', () => {
            const value = new Uuid4Value(undefined);
            expect(value.toString()).toBe('none');
        });
    });

    describe('equals', () => {
        it('should compare equal UUIDs', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value1 = new Uuid4Value(uuid);
            const value2 = new Uuid4Value(uuid);
            expect(value1.equals(value2)).toBe(true);
        });

        it('should compare different UUIDs', () => {
            const value1 = Uuid4Value.generate();
            const value2 = Uuid4Value.generate();
            expect(value1.equals(value2)).toBe(false);
        });

        it('should compare undefined UUIDs', () => {
            const value1 = new Uuid4Value(undefined);
            const value2 = new Uuid4Value(undefined);
            expect(value1.equals(value2)).toBe(true);
        });
    });

    describe('compare', () => {
        it('should order UUIDs consistently', () => {
            const value1 = Uuid4Value.generate();
            const value2 = Uuid4Value.generate();
            
            const cmp1 = value1.compare(value2);
            const cmp2 = value1.compare(value2);
            expect(cmp1).toBe(cmp2);
        });

        it('should return 0 for equal UUIDs', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value1 = new Uuid4Value(uuid);
            const value2 = new Uuid4Value(uuid);
            expect(value1.compare(value2)).toBe(0);
        });

        it('should handle undefined values', () => {
            const value1 = new Uuid4Value(undefined);
            const value2 = Uuid4Value.generate();
            const value3 = new Uuid4Value(undefined);
            
            expect(value1.compare(value2)).toBe(-1);
            expect(value2.compare(value1)).toBe(1);
            expect(value1.compare(value3)).toBe(0);
        });
    });

    describe('valueOf', () => {
        it('should return the UUID string', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = new Uuid4Value(uuid);
            expect(value.valueOf()).toBe(uuid);
        });

        it('should return undefined when value is undefined', () => {
            const value = new Uuid4Value(undefined);
            expect(value.valueOf()).toBeUndefined();
        });
    });
});