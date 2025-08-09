/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Uint8Value} from '../../src/value/uint8';

describe('Uint8Value', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const uint8 = new Uint8Value(BigInt(1000000));
            expect(uint8.value).toBe(BigInt(1000000));
            expect(uint8.type).toBe('Uint8');
        });

        it('should create instance with valid number value', () => {
            const uint8 = new Uint8Value(1000000);
            expect(uint8.value).toBe(BigInt(1000000));
            expect(uint8.type).toBe('Uint8');
        });

        it('should truncate decimal number to integer', () => {
            const uint8 = new Uint8Value(42.9);
            expect(uint8.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const uint8 = new Uint8Value(undefined);
            expect(uint8.value).toBeUndefined();
            expect(uint8.type).toBe('Uint8');
        });

        it('should create instance with no arguments', () => {
            const uint8 = new Uint8Value();
            expect(uint8.value).toBeUndefined();
            expect(uint8.type).toBe('Uint8');
        });

        it('should accept minimum value 0', () => {
            const uint8 = new Uint8Value(BigInt(0));
            expect(uint8.value).toBe(BigInt(0));
        });

        it('should accept maximum value 18446744073709551615', () => {
            const uint8 = new Uint8Value(BigInt("18446744073709551615"));
            expect(uint8.value).toBe(BigInt("18446744073709551615"));
        });

        it('should accept large positive number', () => {
            const uint8 = new Uint8Value(BigInt("10000000000000000"));
            expect(uint8.value).toBe(BigInt("10000000000000000"));
        });

        it('should throw error for negative value', () => {
            expect(() => new Uint8Value(BigInt(-1))).toThrow('Uint8 value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Uint8Value(BigInt("18446744073709551616"))).toThrow('Uint8 value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const uint8 = Uint8Value.parse('1000000');
            expect(uint8.value).toBe(BigInt(1000000));
        });

        it('should parse minimum value string', () => {
            const uint8 = Uint8Value.parse('0');
            expect(uint8.value).toBe(BigInt(0));
        });

        it('should parse maximum value string', () => {
            const uint8 = Uint8Value.parse('18446744073709551615');
            expect(uint8.value).toBe(BigInt("18446744073709551615"));
        });

        it('should parse large positive number string', () => {
            const uint8 = Uint8Value.parse('10000000000000000');
            expect(uint8.value).toBe(BigInt("10000000000000000"));
        });

        it('should trim whitespace', () => {
            const uint8 = Uint8Value.parse('  1000000  ');
            expect(uint8.value).toBe(BigInt(1000000));
        });

        it('should return undefined for empty string', () => {
            const uint8 = Uint8Value.parse('');
            expect(uint8.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const uint8 = Uint8Value.parse('   ');
            expect(uint8.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Uint8Value.parse('abc')).toThrow('Cannot parse "abc" as Uint8');
        });

        it('should throw error for negative value string', () => {
            expect(() => Uint8Value.parse('-1')).toThrow('Uint8 value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Uint8Value.parse('18446744073709551616')).toThrow('Uint8 value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });

        it('should throw error for decimal string', () => {
            expect(() => Uint8Value.parse('42.5')).toThrow('Cannot parse "42.5" as Uint8');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const uint8 = new Uint8Value(BigInt(1000000));
            expect(uint8.valueOf()).toBe(BigInt(1000000));
        });

        it('should return undefined when value is undefined', () => {
            const uint8 = new Uint8Value(undefined);
            expect(uint8.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const uint8 = new Uint8Value(0);
            expect(uint8.valueOf()).toBe(BigInt(0));
        });
    });

    describe('toNumber', () => {
        it('should convert bigint to number', () => {
            const uint8 = new Uint8Value(BigInt(1000000));
            expect(uint8.toNumber()).toBe(1000000);
        });

        it('should return undefined when value is undefined', () => {
            const uint8 = new Uint8Value(undefined);
            expect(uint8.toNumber()).toBeUndefined();
        });

        it('should convert zero to number', () => {
            const uint8 = new Uint8Value(BigInt(0));
            expect(uint8.toNumber()).toBe(0);
        });

        it('should convert large value to number (with potential precision loss)', () => {
            const uint8 = new Uint8Value(BigInt("9007199254740993")); // Number.MAX_SAFE_INTEGER + 2
            const result = uint8.toNumber();
            expect(typeof result).toBe('number');
        });
    });
});