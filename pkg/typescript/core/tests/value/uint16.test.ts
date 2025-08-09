/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Uint16Value} from '../../src/value/uint16';

describe('Uint16Value', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const uint16 = new Uint16Value(BigInt(1000000));
            expect(uint16.value).toBe(BigInt(1000000));
            expect(uint16.type).toBe('Uint16');
        });

        it('should create instance with valid number value', () => {
            const uint16 = new Uint16Value(1000000);
            expect(uint16.value).toBe(BigInt(1000000));
            expect(uint16.type).toBe('Uint16');
        });

        it('should create instance with valid string value', () => {
            const uint16 = new Uint16Value("1000000");
            expect(uint16.value).toBe(BigInt(1000000));
            expect(uint16.type).toBe('Uint16');
        });

        it('should truncate decimal number to integer', () => {
            const uint16 = new Uint16Value(42.9);
            expect(uint16.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const uint16 = new Uint16Value(undefined);
            expect(uint16.value).toBeUndefined();
            expect(uint16.type).toBe('Uint16');
        });

        it('should create instance with no arguments', () => {
            const uint16 = new Uint16Value();
            expect(uint16.value).toBeUndefined();
            expect(uint16.type).toBe('Uint16');
        });

        it('should accept minimum value 0', () => {
            const uint16 = new Uint16Value(BigInt(0));
            expect(uint16.value).toBe(BigInt(0));
        });

        it('should accept maximum value 340282366920938463463374607431768211455', () => {
            const uint16 = new Uint16Value(BigInt("340282366920938463463374607431768211455"));
            expect(uint16.value).toBe(BigInt("340282366920938463463374607431768211455"));
        });

        it('should accept very large positive number', () => {
            const uint16 = new Uint16Value(BigInt("100000000000000000000000000000000000"));
            expect(uint16.value).toBe(BigInt("100000000000000000000000000000000000"));
        });

        it('should accept number beyond JavaScript safe integer range', () => {
            const uint16 = new Uint16Value("99999999999999999999999999999999999999");
            expect(uint16.value).toBe(BigInt("99999999999999999999999999999999999999"));
        });

        it('should throw error for invalid string value', () => {
            expect(() => new Uint16Value("not a number")).toThrow('Uint16 value must be a valid integer, got not a number');
        });

        it('should throw error for negative value', () => {
            expect(() => new Uint16Value(BigInt(-1))).toThrow('Uint16 value must be between 0 and 340282366920938463463374607431768211455, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Uint16Value(BigInt("340282366920938463463374607431768211456"))).toThrow('Uint16 value must be between 0 and 340282366920938463463374607431768211455, got 340282366920938463463374607431768211456');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const uint16 = Uint16Value.parse('1000000');
            expect(uint16.value).toBe(BigInt(1000000));
        });

        it('should parse minimum value string', () => {
            const uint16 = Uint16Value.parse('0');
            expect(uint16.value).toBe(BigInt(0));
        });

        it('should parse maximum value string', () => {
            const uint16 = Uint16Value.parse('340282366920938463463374607431768211455');
            expect(uint16.value).toBe(BigInt("340282366920938463463374607431768211455"));
        });

        it('should parse very large positive number string', () => {
            const uint16 = Uint16Value.parse('100000000000000000000000000000000000');
            expect(uint16.value).toBe(BigInt("100000000000000000000000000000000000"));
        });

        it('should trim whitespace', () => {
            const uint16 = Uint16Value.parse('  1000000  ');
            expect(uint16.value).toBe(BigInt(1000000));
        });

        it('should return undefined for empty string', () => {
            const uint16 = Uint16Value.parse('');
            expect(uint16.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const uint16 = Uint16Value.parse('   ');
            expect(uint16.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Uint16Value.parse('abc')).toThrow('Cannot parse "abc" as Uint16');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Uint16Value.parse('1000000abc')).toThrow('Cannot parse "1000000abc" as Uint16');
        });

        it('should throw error for negative value string', () => {
            expect(() => Uint16Value.parse('-1')).toThrow('Uint16 value must be between 0 and 340282366920938463463374607431768211455, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Uint16Value.parse('340282366920938463463374607431768211456')).toThrow('Uint16 value must be between 0 and 340282366920938463463374607431768211455, got 340282366920938463463374607431768211456');
        });

        it('should throw error for decimal string', () => {
            expect(() => Uint16Value.parse('42.5')).toThrow('Cannot parse "42.5" as Uint16');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const uint16 = new Uint16Value(BigInt(1000000));
            expect(uint16.valueOf()).toBe(BigInt(1000000));
        });

        it('should return undefined when value is undefined', () => {
            const uint16 = new Uint16Value(undefined);
            expect(uint16.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const uint16 = new Uint16Value(0);
            expect(uint16.valueOf()).toBe(BigInt(0));
        });
    });

    describe('toString', () => {
        it('should convert bigint to string', () => {
            const uint16 = new Uint16Value(BigInt(1000000));
            expect(uint16.toString()).toBe('1000000');
        });

        it('should return "undefined" when value is undefined', () => {
            const uint16 = new Uint16Value(undefined);
            expect(uint16.toString()).toBe('undefined');
        });

        it('should convert zero to string', () => {
            const uint16 = new Uint16Value(BigInt(0));
            expect(uint16.toString()).toBe('0');
        });

        it('should convert very large number to string', () => {
            const uint16 = new Uint16Value(BigInt("100000000000000000000000000000000000"));
            expect(uint16.toString()).toBe('100000000000000000000000000000000000');
        });
    });
});