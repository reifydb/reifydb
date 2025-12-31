// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {Int16Value} from '../../src';

describe('Int16Value', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const int16 = new Int16Value(BigInt(100000));
            expect(int16.value).toBe(BigInt(100000));
            expect(int16.type).toBe('Int16');
        });

        it('should create instance with valid number value', () => {
            const int16 = new Int16Value(100000);
            expect(int16.value).toBe(BigInt(100000));
            expect(int16.type).toBe('Int16');
        });

        it('should create instance with valid string value', () => {
            const int16 = new Int16Value("100000");
            expect(int16.value).toBe(BigInt(100000));
            expect(int16.type).toBe('Int16');
        });

        it('should truncate decimal number to integer', () => {
            const int16 = new Int16Value(42.9);
            expect(int16.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const int16 = new Int16Value(undefined);
            expect(int16.value).toBeUndefined();
            expect(int16.type).toBe('Int16');
        });

        it('should create instance with no arguments', () => {
            const int16 = new Int16Value();
            expect(int16.value).toBeUndefined();
            expect(int16.type).toBe('Int16');
        });

        it('should accept minimum value -170141183460469231731687303715884105728', () => {
            const int16 = new Int16Value(BigInt("-170141183460469231731687303715884105728"));
            expect(int16.value).toBe(BigInt("-170141183460469231731687303715884105728"));
        });

        it('should accept maximum value 170141183460469231731687303715884105727', () => {
            const int16 = new Int16Value(BigInt("170141183460469231731687303715884105727"));
            expect(int16.value).toBe(BigInt("170141183460469231731687303715884105727"));
        });

        it('should accept zero', () => {
            const int16 = new Int16Value(0);
            expect(int16.value).toBe(BigInt(0));
        });

        it('should accept very large positive number', () => {
            const int16 = new Int16Value(BigInt("100000000000000000000000000000000000"));
            expect(int16.value).toBe(BigInt("100000000000000000000000000000000000"));
        });

        it('should accept very large negative number', () => {
            const int16 = new Int16Value(BigInt("-100000000000000000000000000000000000"));
            expect(int16.value).toBe(BigInt("-100000000000000000000000000000000000"));
        });

        it('should accept number beyond JavaScript safe integer range', () => {
            const int16 = new Int16Value("9999999999999999999999999999999999999");
            expect(int16.value).toBe(BigInt("9999999999999999999999999999999999999"));
        });

        it('should throw error for invalid string value', () => {
            expect(() => new Int16Value("not a number")).toThrow('Int16 value must be a valid integer, got not a number');
        });

        it('should throw error for value below minimum', () => {
            expect(() => new Int16Value(BigInt("-170141183460469231731687303715884105729"))).toThrow('Int16 value must be between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727, got -170141183460469231731687303715884105729');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Int16Value(BigInt("170141183460469231731687303715884105728"))).toThrow('Int16 value must be between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727, got 170141183460469231731687303715884105728');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const int16 = Int16Value.parse('100000');
            expect(int16.value).toBe(BigInt(100000));
        });

        it('should parse negative integer string', () => {
            const int16 = Int16Value.parse('-100000');
            expect(int16.value).toBe(BigInt(-100000));
        });

        it('should parse minimum value string', () => {
            const int16 = Int16Value.parse('-170141183460469231731687303715884105728');
            expect(int16.value).toBe(BigInt("-170141183460469231731687303715884105728"));
        });

        it('should parse maximum value string', () => {
            const int16 = Int16Value.parse('170141183460469231731687303715884105727');
            expect(int16.value).toBe(BigInt("170141183460469231731687303715884105727"));
        });

        it('should parse zero string', () => {
            const int16 = Int16Value.parse('0');
            expect(int16.value).toBe(BigInt(0));
        });

        it('should parse very large positive number string', () => {
            const int16 = Int16Value.parse('100000000000000000000000000000000000');
            expect(int16.value).toBe(BigInt("100000000000000000000000000000000000"));
        });

        it('should parse very large negative number string', () => {
            const int16 = Int16Value.parse('-100000000000000000000000000000000000');
            expect(int16.value).toBe(BigInt("-100000000000000000000000000000000000"));
        });

        it('should trim whitespace', () => {
            const int16 = Int16Value.parse('  100000  ');
            expect(int16.value).toBe(BigInt(100000));
        });

        it('should return undefined for empty string', () => {
            const int16 = Int16Value.parse('');
            expect(int16.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const int16 = Int16Value.parse('   ');
            expect(int16.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Int16Value.parse('abc')).toThrow('Cannot parse "abc" as Int16');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Int16Value.parse('100000abc')).toThrow('Cannot parse "100000abc" as Int16');
        });

        it('should throw error for value outside range', () => {
            expect(() => Int16Value.parse('170141183460469231731687303715884105728')).toThrow('Int16 value must be between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727, got 170141183460469231731687303715884105728');
        });

        it('should throw error for decimal string', () => {
            expect(() => Int16Value.parse('42.5')).toThrow('Cannot parse "42.5" as Int16');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const int16 = new Int16Value(BigInt(100000));
            expect(int16.valueOf()).toBe(BigInt(100000));
        });

        it('should return undefined when value is undefined', () => {
            const int16 = new Int16Value(undefined);
            expect(int16.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const int16 = new Int16Value(BigInt(-100000));
            expect(int16.valueOf()).toBe(BigInt(-100000));
        });

        it('should return zero', () => {
            const int16 = new Int16Value(0);
            expect(int16.valueOf()).toBe(BigInt(0));
        });
    });
});