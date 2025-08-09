/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Int2Value} from '../../src/value/int2';

describe('Int2Value', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const int2 = new Int2Value(1000);
            expect(int2.value).toBe(1000);
            expect(int2.type).toBe('Int2');
        });

        it('should create instance with undefined value', () => {
            const int2 = new Int2Value(undefined);
            expect(int2.value).toBeUndefined();
            expect(int2.type).toBe('Int2');
        });

        it('should create instance with no arguments', () => {
            const int2 = new Int2Value();
            expect(int2.value).toBeUndefined();
            expect(int2.type).toBe('Int2');
        });

        it('should accept minimum value -32768', () => {
            const int2 = new Int2Value(-32768);
            expect(int2.value).toBe(-32768);
        });

        it('should accept maximum value 32767', () => {
            const int2 = new Int2Value(32767);
            expect(int2.value).toBe(32767);
        });

        it('should accept zero', () => {
            const int2 = new Int2Value(0);
            expect(int2.value).toBe(0);
        });

        it('should throw error for value below minimum', () => {
            expect(() => new Int2Value(-32769)).toThrow('Int2 value must be between -32768 and 32767, got -32769');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Int2Value(32768)).toThrow('Int2 value must be between -32768 and 32767, got 32768');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Int2Value(42.5)).toThrow('Int2 value must be an integer, got 42.5');
        });

        it('should throw error for decimal close to integer', () => {
            expect(() => new Int2Value(1000.001)).toThrow('Int2 value must be an integer, got 1000.001');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const int2 = Int2Value.parse('1000');
            expect(int2.value).toBe(1000);
        });

        it('should parse negative integer string', () => {
            const int2 = Int2Value.parse('-1000');
            expect(int2.value).toBe(-1000);
        });

        it('should parse minimum value string', () => {
            const int2 = Int2Value.parse('-32768');
            expect(int2.value).toBe(-32768);
        });

        it('should parse maximum value string', () => {
            const int2 = Int2Value.parse('32767');
            expect(int2.value).toBe(32767);
        });

        it('should parse zero string', () => {
            const int2 = Int2Value.parse('0');
            expect(int2.value).toBe(0);
        });

        it('should trim whitespace', () => {
            const int2 = Int2Value.parse('  1000  ');
            expect(int2.value).toBe(1000);
        });

        it('should return undefined for empty string', () => {
            const int2 = Int2Value.parse('');
            expect(int2.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const int2 = Int2Value.parse('   ');
            expect(int2.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Int2Value.parse('abc')).toThrow('Cannot parse "abc" as Int2');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Int2Value.parse('1000abc')).toThrow('Cannot parse "1000abc" as Int2');
        });

        it('should throw error for value outside range', () => {
            expect(() => Int2Value.parse('32768')).toThrow('Int2 value must be between -32768 and 32767, got 32768');
        });

        it('should throw error for decimal string', () => {
            expect(() => Int2Value.parse('42.5')).toThrow('Int2 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const int2 = new Int2Value(1000);
            expect(int2.valueOf()).toBe(1000);
        });

        it('should return undefined when value is undefined', () => {
            const int2 = new Int2Value(undefined);
            expect(int2.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const int2 = new Int2Value(-1000);
            expect(int2.valueOf()).toBe(-1000);
        });

        it('should return zero', () => {
            const int2 = new Int2Value(0);
            expect(int2.valueOf()).toBe(0);
        });
    });
});