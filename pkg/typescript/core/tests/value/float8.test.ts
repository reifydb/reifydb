/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Float8} from '../../src/value/float8';

describe('Float8', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const float8 = new Float8(3.141592653589793);
            expect(float8.value).toBe(3.141592653589793);
            expect(float8.type).toBe('Float8');
        });

        it('should create instance with undefined value', () => {
            const float8 = new Float8(undefined);
            expect(float8.value).toBeUndefined();
            expect(float8.type).toBe('Float8');
        });

        it('should create instance with no arguments', () => {
            const float8 = new Float8();
            expect(float8.value).toBeUndefined();
            expect(float8.type).toBe('Float8');
        });

        it('should accept zero', () => {
            const float8 = new Float8(0);
            expect(float8.value).toBe(0);
        });

        it('should accept negative zero', () => {
            const float8 = new Float8(-0);
            expect(float8.value).toBe(-0);
            expect(Object.is(float8.value, -0)).toBe(true);
        });

        it('should preserve Float64 precision', () => {
            const float8 = new Float8(3.141592653589793);
            expect(float8.value).toBe(3.141592653589793);
        });

        it('should handle positive infinity', () => {
            const float8 = new Float8(Infinity);
            expect(float8.value).toBe(Infinity);
        });

        it('should handle negative infinity', () => {
            const float8 = new Float8(-Infinity);
            expect(float8.value).toBe(-Infinity);
        });

        it('should handle NaN', () => {
            const float8 = new Float8(NaN);
            expect(float8.value).toBeNaN();
        });

        it('should handle Number.MAX_VALUE', () => {
            const float8 = new Float8(Number.MAX_VALUE);
            expect(float8.value).toBe(Number.MAX_VALUE);
        });

        it('should handle Number.MIN_VALUE', () => {
            const float8 = new Float8(Number.MIN_VALUE);
            expect(float8.value).toBe(Number.MIN_VALUE);
        });

        it('should handle negative Number.MAX_VALUE', () => {
            const float8 = new Float8(-Number.MAX_VALUE);
            expect(float8.value).toBe(-Number.MAX_VALUE);
        });

        it('should throw error for non-number value', () => {
            expect(() => new Float8("123" as any)).toThrow('Float8 value must be a number, got string');
        });
    });

    describe('parse', () => {
        it('should parse valid float string', () => {
            const float8 = Float8.parse('3.141592653589793');
            expect(float8.value).toBe(3.141592653589793);
        });

        it('should parse negative float string', () => {
            const float8 = Float8.parse('-3.141592653589793');
            expect(float8.value).toBe(-3.141592653589793);
        });

        it('should parse zero string', () => {
            const float8 = Float8.parse('0');
            expect(float8.value).toBe(0);
        });

        it('should parse exponential notation', () => {
            const float8 = Float8.parse('1.23e-10');
            expect(float8.value).toBe(1.23e-10);
        });

        it('should parse large exponential notation', () => {
            const float8 = Float8.parse('1.5e308');
            expect(float8.value).toBe(1.5e308);
        });

        it('should parse infinity string', () => {
            const float8 = Float8.parse('Infinity');
            expect(float8.value).toBe(Infinity);
        });

        it('should parse negative infinity string', () => {
            const float8 = Float8.parse('-Infinity');
            expect(float8.value).toBe(-Infinity);
        });

        it('should parse NaN string', () => {
            const float8 = Float8.parse('NaN');
            expect(float8.value).toBeNaN();
        });

        it('should trim whitespace', () => {
            const float8 = Float8.parse('  3.14  ');
            expect(float8.value).toBe(3.14);
        });

        it('should return undefined for empty string', () => {
            const float8 = Float8.parse('');
            expect(float8.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const float8 = Float8.parse('   ');
            expect(float8.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Float8.parse('abc')).toThrow('Cannot parse "abc" as Float8');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Float8.parse('3.14abc')).toThrow('Cannot parse "3.14abc" as Float8');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const float8 = new Float8(3.141592653589793);
            expect(float8.valueOf()).toBe(3.141592653589793);
        });

        it('should return undefined when value is undefined', () => {
            const float8 = new Float8(undefined);
            expect(float8.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const float8 = new Float8(0);
            expect(float8.valueOf()).toBe(0);
        });
    });
});