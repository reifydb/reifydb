/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Int4} from '../../src/value/int4';

describe('Int4', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const int4 = new Int4(100000);
            expect(int4.value).toBe(100000);
            expect(int4.type).toBe('Int4');
        });

        it('should create instance with undefined value', () => {
            const int4 = new Int4(undefined);
            expect(int4.value).toBeUndefined();
            expect(int4.type).toBe('Int4');
        });

        it('should create instance with no arguments', () => {
            const int4 = new Int4();
            expect(int4.value).toBeUndefined();
            expect(int4.type).toBe('Int4');
        });

        it('should accept minimum value -2147483648', () => {
            const int4 = new Int4(-2147483648);
            expect(int4.value).toBe(-2147483648);
        });

        it('should accept maximum value 2147483647', () => {
            const int4 = new Int4(2147483647);
            expect(int4.value).toBe(2147483647);
        });

        it('should accept zero', () => {
            const int4 = new Int4(0);
            expect(int4.value).toBe(0);
        });

        it('should throw error for value below minimum', () => {
            expect(() => new Int4(-2147483649)).toThrow('Int4 value must be between -2147483648 and 2147483647, got -2147483649');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Int4(2147483648)).toThrow('Int4 value must be between -2147483648 and 2147483647, got 2147483648');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Int4(42.5)).toThrow('Int4 value must be an integer, got 42.5');
        });

        it('should throw error for decimal close to integer', () => {
            expect(() => new Int4(100000.001)).toThrow('Int4 value must be an integer, got 100000.001');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const int4 = Int4.parse('100000');
            expect(int4.value).toBe(100000);
        });

        it('should parse negative integer string', () => {
            const int4 = Int4.parse('-100000');
            expect(int4.value).toBe(-100000);
        });

        it('should parse minimum value string', () => {
            const int4 = Int4.parse('-2147483648');
            expect(int4.value).toBe(-2147483648);
        });

        it('should parse maximum value string', () => {
            const int4 = Int4.parse('2147483647');
            expect(int4.value).toBe(2147483647);
        });

        it('should parse zero string', () => {
            const int4 = Int4.parse('0');
            expect(int4.value).toBe(0);
        });

        it('should trim whitespace', () => {
            const int4 = Int4.parse('  100000  ');
            expect(int4.value).toBe(100000);
        });

        it('should return undefined for empty string', () => {
            const int4 = Int4.parse('');
            expect(int4.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const int4 = Int4.parse('   ');
            expect(int4.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Int4.parse('abc')).toThrow('Cannot parse "abc" as Int4');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Int4.parse('100000abc')).toThrow('Cannot parse "100000abc" as Int4');
        });

        it('should throw error for value outside range', () => {
            expect(() => Int4.parse('2147483648')).toThrow('Int4 value must be between -2147483648 and 2147483647, got 2147483648');
        });

        it('should throw error for decimal string', () => {
            expect(() => Int4.parse('42.5')).toThrow('Int4 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const int4 = new Int4(100000);
            expect(int4.valueOf()).toBe(100000);
        });

        it('should return undefined when value is undefined', () => {
            const int4 = new Int4(undefined);
            expect(int4.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const int4 = new Int4(-100000);
            expect(int4.valueOf()).toBe(-100000);
        });

        it('should return zero', () => {
            const int4 = new Int4(0);
            expect(int4.valueOf()).toBe(0);
        });
    });
});