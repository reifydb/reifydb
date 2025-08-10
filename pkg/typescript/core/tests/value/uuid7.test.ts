/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { Uuid7Value } from '../../src/value/uuid7';
import { NIL as NIL_UUID } from 'uuid';

describe('Uuid7Value', () => {
    describe('constructor', () => {
        it('should create instance with valid UUID v7', () => {
            // This is a sample UUID v7 (you can generate real ones with uuidv7())
            const uuid = '018c5f3a-8b86-7a98-a79f-123456789abc';
            const value = new Uuid7Value(uuid);
            expect(value.asString()).toBe(uuid);
        });

        it('should create instance with nil UUID', () => {
            const value = new Uuid7Value(NIL_UUID);
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });

        it('should create instance with undefined', () => {
            const value = new Uuid7Value(undefined);
            expect(value.asString()).toBeUndefined();
        });

        it('should convert to lowercase', () => {
            const uuid = '018C5F3A-8B86-7A98-A79F-123456789ABC';
            const value = new Uuid7Value(uuid);
            expect(value.asString()).toBe(uuid.toLowerCase());
        });

        it('should throw error for invalid UUID format', () => {
            expect(() => new Uuid7Value('not-a-uuid')).toThrow('Invalid UUID format');
            expect(() => new Uuid7Value('018c5f3a-8b86-7a98-a79f')).toThrow('Invalid UUID format');
        });

        it('should throw error for wrong UUID version', () => {
            // This is a UUID v4
            const uuidV4 = '550e8400-e29b-41d4-a716-446655440000';
            expect(() => new Uuid7Value(uuidV4)).toThrow('Invalid UUID version for Uuid7');
        });

        it('should throw error for non-string value', () => {
            expect(() => new Uuid7Value(123 as any)).toThrow('Uuid7 value must be a string');
        });
    });

    describe('generate', () => {
        it('should generate a new UUID v7', () => {
            const value = Uuid7Value.generate();
            expect(value.asString()).toBeDefined();
            expect(value.getVersion()).toBe(7);
        });

        it('should generate unique UUIDs', () => {
            const value1 = Uuid7Value.generate();
            const value2 = Uuid7Value.generate();
            expect(value1.asString()).not.toBe(value2.asString());
        });

        it('should generate UUIDs with increasing timestamps', async () => {
            const value1 = Uuid7Value.generate();
            await new Promise(resolve => setTimeout(resolve, 2));
            const value2 = Uuid7Value.generate();
            
            const timestamp1 = value1.getTimestamp();
            const timestamp2 = value2.getTimestamp();
            
            expect(timestamp1).toBeDefined();
            expect(timestamp2).toBeDefined();
            expect(timestamp2! >= timestamp1!).toBe(true);
        });
    });

    describe('nil', () => {
        it('should create nil UUID', () => {
            const value = Uuid7Value.nil();
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });
    });

    describe('default', () => {
        it('should return nil UUID as default', () => {
            const value = Uuid7Value.default();
            expect(value.asString()).toBe(NIL_UUID);
            expect(value.isNil()).toBe(true);
        });
    });

    describe('parse', () => {
        it('should parse valid UUID v7 string', () => {
            const value1 = Uuid7Value.generate();
            const uuid = value1.asString()!;
            const value2 = Uuid7Value.parse(uuid);
            expect(value2.asString()).toBe(uuid);
        });

        it('should parse nil UUID', () => {
            const value = Uuid7Value.parse(NIL_UUID);
            expect(value.asString()).toBe(NIL_UUID);
        });

        it('should parse with whitespace', () => {
            const value1 = Uuid7Value.generate();
            const uuid = value1.asString()!;
            const value2 = Uuid7Value.parse(`  ${uuid}  `);
            expect(value2.asString()).toBe(uuid);
        });

        it('should return undefined for empty string', () => {
            expect(Uuid7Value.parse('').value).toBeUndefined();
            expect(Uuid7Value.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(Uuid7Value.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid UUID', () => {
            expect(() => Uuid7Value.parse('not-a-uuid')).toThrow('Cannot parse');
        });

        it('should throw error for wrong version', () => {
            // UUID v4
            const uuidV4 = '550e8400-e29b-41d4-a716-446655440000';
            expect(() => Uuid7Value.parse(uuidV4)).toThrow('wrong version');
        });
    });

    describe('asBytes', () => {
        it('should convert UUID to 16-byte array', () => {
            const value = Uuid7Value.generate();
            const bytes = value.asBytes();
            
            expect(bytes).toBeInstanceOf(Uint8Array);
            expect(bytes?.length).toBe(16);
        });

        it('should return undefined for undefined UUID', () => {
            const value = new Uuid7Value(undefined);
            expect(value.asBytes()).toBeUndefined();
        });
    });

    describe('getTimestamp', () => {
        it('should extract timestamp from UUID v7', () => {
            const value = Uuid7Value.generate();
            const timestamp = value.getTimestamp();
            
            expect(timestamp).toBeDefined();
            expect(typeof timestamp).toBe('number');
            expect(timestamp! > 0).toBe(true);
            
            // Should be close to current time (within last minute)
            const now = Date.now();
            expect(Math.abs(now - timestamp!) < 60000).toBe(true);
        });

        it('should return undefined for nil UUID', () => {
            const value = Uuid7Value.nil();
            expect(value.getTimestamp()).toBeUndefined();
        });

        it('should return undefined for undefined UUID', () => {
            const value = new Uuid7Value(undefined);
            expect(value.getTimestamp()).toBeUndefined();
        });
    });

    describe('getVersion', () => {
        it('should return version 7 for v7 UUID', () => {
            const value = Uuid7Value.generate();
            expect(value.getVersion()).toBe(7);
        });

        it('should return 0 for nil UUID', () => {
            const value = Uuid7Value.nil();
            expect(value.getVersion()).toBe(0);
        });

        it('should return undefined for undefined UUID', () => {
            const value = new Uuid7Value(undefined);
            expect(value.getVersion()).toBeUndefined();
        });
    });

    describe('toString', () => {
        it('should format UUID as string', () => {
            const value = Uuid7Value.generate();
            const uuid = value.asString()!;
            expect(value.toString()).toBe(uuid);
        });

        it('should format undefined as "undefined"', () => {
            const value = new Uuid7Value(undefined);
            expect(value.toString()).toBe('undefined');
        });
    });

    describe('equals', () => {
        it('should compare equal UUIDs', () => {
            const value1 = Uuid7Value.generate();
            const uuid = value1.asString()!;
            const value2 = new Uuid7Value(uuid);
            expect(value1.equals(value2)).toBe(true);
        });

        it('should compare different UUIDs', () => {
            const value1 = Uuid7Value.generate();
            const value2 = Uuid7Value.generate();
            expect(value1.equals(value2)).toBe(false);
        });

        it('should compare undefined UUIDs', () => {
            const value1 = new Uuid7Value(undefined);
            const value2 = new Uuid7Value(undefined);
            expect(value1.equals(value2)).toBe(true);
        });
    });

    describe('compare', () => {
        it('should order UUIDs consistently', () => {
            const value1 = Uuid7Value.generate();
            const value2 = Uuid7Value.generate();
            
            const cmp1 = value1.compare(value2);
            const cmp2 = value1.compare(value2);
            expect(cmp1).toBe(cmp2);
        });

        it('should return 0 for equal UUIDs', () => {
            const value1 = Uuid7Value.generate();
            const uuid = value1.asString()!;
            const value2 = new Uuid7Value(uuid);
            expect(value1.compare(value2)).toBe(0);
        });

        it('should handle undefined values', () => {
            const value1 = new Uuid7Value(undefined);
            const value2 = Uuid7Value.generate();
            const value3 = new Uuid7Value(undefined);
            
            expect(value1.compare(value2)).toBe(-1);
            expect(value2.compare(value1)).toBe(1);
            expect(value1.compare(value3)).toBe(0);
        });

        it('should order by timestamp for sequential UUIDs', async () => {
            const value1 = Uuid7Value.generate();
            await new Promise(resolve => setTimeout(resolve, 2));
            const value2 = Uuid7Value.generate();
            
            // Earlier UUID should be less than later UUID
            expect(value1.compare(value2)).toBe(-1);
            expect(value2.compare(value1)).toBe(1);
        });
    });

    describe('valueOf', () => {
        it('should return the UUID string', () => {
            const value = Uuid7Value.generate();
            const uuid = value.asString();
            expect(value.valueOf()).toBe(uuid);
        });

        it('should return undefined when value is undefined', () => {
            const value = new Uuid7Value(undefined);
            expect(value.valueOf()).toBeUndefined();
        });
    });
});