/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Float4} from '../../src/value/float4';

describe('Float4', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const float4 = new Float4(3.14159);
            expect(float4.value).toBeCloseTo(3.14159, 5);
            expect(float4.type).toBe('Float4');
        });

        it('should create instance with undefined value', () => {
            const float4 = new Float4(undefined);
            expect(float4.value).toBeUndefined();
            expect(float4.type).toBe('Float4');
        });

        it('should create instance with no arguments', () => {
            const float4 = new Float4();
            expect(float4.value).toBeUndefined();
            expect(float4.type).toBe('Float4');
        });

        it('should accept zero', () => {
            const float4 = new Float4(0);
            expect(float4.value).toBe(0);
        });

        it('should accept negative zero', () => {
            const float4 = new Float4(-0);
            expect(float4.value).toBe(-0);
            expect(Object.is(float4.value, -0)).toBe(true);
        });

        it('should convert to Float32 precision', () => {
            // This number has more precision than Float32 can handle
            const float4 = new Float4(3.141592653589793);
            // Float32 precision is about 7 decimal digits
            expect(float4.value).not.toBe(3.141592653589793);
            expect(float4.value).toBeCloseTo(3.1415927, 6);
        });

        it('should handle positive infinity', () => {
            const float4 = new Float4(Infinity);
            expect(float4.value).toBeUndefined();
        });

        it('should handle negative infinity', () => {
            const float4 = new Float4(-Infinity);
            expect(float4.value).toBeUndefined();
        });

        it('should handle NaN', () => {
            const float4 = new Float4(NaN);
            expect(float4.value).toBeUndefined();
        });

        it('should throw error for values above max', () => {
            expect(() => new Float4(3.5e38)).toThrow('Float4 overflow: value 3.5e+38 exceeds maximum 3.4028235e+38');
        });

        it('should throw error for values below min', () => {
            expect(() => new Float4(-3.5e38)).toThrow('Float4 underflow: value -3.5e+38 exceeds minimum -3.4028235e+38');
        });

        it('should underflow to zero for very small positive values', () => {
            const float4 = new Float4(1e-39);
            expect(float4.value).toBe(0);
        });

        it('should underflow to zero for very small negative values', () => {
            const float4 = new Float4(-1e-39);
            expect(float4.value).toBe(0);
        });

        it('should preserve values near Float32 min positive', () => {
            const float4 = new Float4(1.175494e-38);
            expect(float4.value).toBeGreaterThan(0);
            expect(float4.value).toBeLessThan(1.2e-38);
        });

        it('should throw error for non-number value', () => {
            expect(() => new Float4("123" as any)).toThrow('Float4 value must be a number, got string');
        });
    });

    describe('parse', () => {
        it('should parse valid float string', () => {
            const float4 = Float4.parse('3.14159');
            expect(float4.value).toBeCloseTo(3.14159, 5);
        });

        it('should parse negative float string', () => {
            const float4 = Float4.parse('-3.14159');
            expect(float4.value).toBeCloseTo(-3.14159, 5);
        });

        it('should parse zero string', () => {
            const float4 = Float4.parse('0');
            expect(float4.value).toBe(0);
        });

        it('should parse exponential notation', () => {
            const float4 = Float4.parse('1.23e-5');
            expect(float4.value).toBeCloseTo(0.0000123, 8);
        });

        it('should parse positive exponential notation', () => {
            const float4 = Float4.parse('1.5e10');
            expect(float4.value).approximately(1.5e10, 512);
        });

        it('should parse infinity string', () => {
            const float4 = Float4.parse('Infinity');
            expect(float4.value).toBeUndefined();
        });

        it('should parse negative infinity string', () => {
            const float4 = Float4.parse('-Infinity');
            expect(float4.value).toBeUndefined();
        });

        it('should parse NaN string', () => {
            const float4 = Float4.parse('NaN');
            expect(float4.value).toBeUndefined();
        });

        it('should trim whitespace', () => {
            const float4 = Float4.parse('  3.14  ');
            expect(float4.value).toBeCloseTo(3.14, 5);
        });

        it('should return undefined for empty string', () => {
            const float4 = Float4.parse('');
            expect(float4.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const float4 = Float4.parse('   ');
            expect(float4.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Float4.parse('abc')).toThrow('Cannot parse "abc" as Float4');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Float4.parse('3.14abc')).toThrow('Cannot parse "3.14abc" as Float4');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const float4 = new Float4(3.14);
            expect(float4.valueOf()).toBeCloseTo(3.14, 5);
        });

        it('should return undefined when value is undefined', () => {
            const float4 = new Float4(undefined);
            expect(float4.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const float4 = new Float4(0);
            expect(float4.valueOf()).toBe(0);
        });
    });
});