/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Int8} from '../../src/value/int8';

describe('Int8', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const int8 = new Int8(BigInt(100000));
            expect(int8.value).toBe(BigInt(100000));
            expect(int8.type).toBe('Int8');
        });

        it('should create instance with valid number value', () => {
            const int8 = new Int8(100000);
            expect(int8.value).toBe(BigInt(100000));
            expect(int8.type).toBe('Int8');
        });

        it('should truncate decimal number to integer', () => {
            const int8 = new Int8(42.9);
            expect(int8.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const int8 = new Int8(undefined);
            expect(int8.value).toBeUndefined();
            expect(int8.type).toBe('Int8');
        });

        it('should create instance with no arguments', () => {
            const int8 = new Int8();
            expect(int8.value).toBeUndefined();
            expect(int8.type).toBe('Int8');
        });

        it('should accept minimum value -9223372036854775808', () => {
            const int8 = new Int8(BigInt("-9223372036854775808"));
            expect(int8.value).toBe(BigInt("-9223372036854775808"));
        });

        it('should accept maximum value 9223372036854775807', () => {
            const int8 = new Int8(BigInt("9223372036854775807"));
            expect(int8.value).toBe(BigInt("9223372036854775807"));
        });

        it('should accept zero', () => {
            const int8 = new Int8(0);
            expect(int8.value).toBe(BigInt(0));
        });

        it('should accept large positive number', () => {
            const int8 = new Int8(BigInt("1000000000000000"));
            expect(int8.value).toBe(BigInt("1000000000000000"));
        });

        it('should accept large negative number', () => {
            const int8 = new Int8(BigInt("-1000000000000000"));
            expect(int8.value).toBe(BigInt("-1000000000000000"));
        });

        it('should throw error for value below minimum', () => {
            expect(() => new Int8(BigInt("-9223372036854775809"))).toThrow('Int8 value must be between -9223372036854775808 and 9223372036854775807, got -9223372036854775809');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Int8(BigInt("9223372036854775808"))).toThrow('Int8 value must be between -9223372036854775808 and 9223372036854775807, got 9223372036854775808');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const int8 = Int8.parse('100000');
            expect(int8.value).toBe(BigInt(100000));
        });

        it('should parse negative integer string', () => {
            const int8 = Int8.parse('-100000');
            expect(int8.value).toBe(BigInt(-100000));
        });

        it('should parse minimum value string', () => {
            const int8 = Int8.parse('-9223372036854775808');
            expect(int8.value).toBe(BigInt("-9223372036854775808"));
        });

        it('should parse maximum value string', () => {
            const int8 = Int8.parse('9223372036854775807');
            expect(int8.value).toBe(BigInt("9223372036854775807"));
        });

        it('should parse zero string', () => {
            const int8 = Int8.parse('0');
            expect(int8.value).toBe(BigInt(0));
        });

        it('should parse large positive number string', () => {
            const int8 = Int8.parse('1000000000000000');
            expect(int8.value).toBe(BigInt("1000000000000000"));
        });

        it('should parse large negative number string', () => {
            const int8 = Int8.parse('-1000000000000000');
            expect(int8.value).toBe(BigInt("-1000000000000000"));
        });

        it('should trim whitespace', () => {
            const int8 = Int8.parse('  100000  ');
            expect(int8.value).toBe(BigInt(100000));
        });

        it('should return undefined for empty string', () => {
            const int8 = Int8.parse('');
            expect(int8.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const int8 = Int8.parse('   ');
            expect(int8.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Int8.parse('abc')).toThrow('Cannot parse "abc" as Int8');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Int8.parse('100000abc')).toThrow('Cannot parse "100000abc" as Int8');
        });

        it('should throw error for value outside range', () => {
            expect(() => Int8.parse('9223372036854775808')).toThrow('Int8 value must be between -9223372036854775808 and 9223372036854775807, got 9223372036854775808');
        });

        it('should throw error for decimal string', () => {
            expect(() => Int8.parse('42.5')).toThrow('Cannot parse "42.5" as Int8');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const int8 = new Int8(BigInt(100000));
            expect(int8.valueOf()).toBe(BigInt(100000));
        });

        it('should return undefined when value is undefined', () => {
            const int8 = new Int8(undefined);
            expect(int8.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const int8 = new Int8(BigInt(-100000));
            expect(int8.valueOf()).toBe(BigInt(-100000));
        });

        it('should return zero', () => {
            const int8 = new Int8(0);
            expect(int8.valueOf()).toBe(BigInt(0));
        });
    });
});