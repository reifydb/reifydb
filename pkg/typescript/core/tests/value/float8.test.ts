// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Float8Value} from '../../src';

describe('Float8Value', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const float8 = new Float8Value(3.141592653589793);
            expect(float8.value).toBe(3.141592653589793);
            expect(float8.type).toBe('Float8');
        });

        it('should reject undefined', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new Float8Value(undefined)).toThrow();
        });

        it('should reject no arguments', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new Float8Value()).toThrow();
        });

        it('should accept zero', () => {
            const float8 = new Float8Value(0);
            expect(float8.value).toBe(0);
        });

        it('should accept negative zero', () => {
            const float8 = new Float8Value(-0);
            expect(float8.value).toBe(-0);
            expect(Object.is(float8.value, -0)).toBe(true);
        });

        it('should preserve Float64 precision', () => {
            const float8 = new Float8Value(3.141592653589793);
            expect(float8.value).toBe(3.141592653589793);
        });

        it('should handle positive infinity, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => new Float8Value(Infinity)).toThrow();
        });

        it('should handle negative infinity, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => new Float8Value(-Infinity)).toThrow();
        });

        it('should handle NaN, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => new Float8Value(NaN)).toThrow();
        });

        it('should handle Number.MAX_VALUE', () => {
            const float8 = new Float8Value(Number.MAX_VALUE);
            expect(float8.value).toBe(Number.MAX_VALUE);
        });

        it('should handle Number.MIN_VALUE', () => {
            const float8 = new Float8Value(Number.MIN_VALUE);
            expect(float8.value).toBe(Number.MIN_VALUE);
        });

        it('should handle negative Number.MAX_VALUE', () => {
            const float8 = new Float8Value(-Number.MAX_VALUE);
            expect(float8.value).toBe(-Number.MAX_VALUE);
        });

        it('should throw error for non-number value', () => {
            expect(() => new Float8Value("123" as any)).toThrow('Float8 value must be a number, got string');
        });
    });

    describe('parse', () => {
        it('should parse valid float string', () => {
            const float8 = Float8Value.parse('3.141592653589793');
            expect(float8.value).toBe(3.141592653589793);
        });

        it('should parse negative float string', () => {
            const float8 = Float8Value.parse('-3.141592653589793');
            expect(float8.value).toBe(-3.141592653589793);
        });

        it('should parse zero string', () => {
            const float8 = Float8Value.parse('0');
            expect(float8.value).toBe(0);
        });

        it('should parse exponential notation', () => {
            const float8 = Float8Value.parse('1.23e-10');
            expect(float8.value).toBe(1.23e-10);
        });

        it('should parse large exponential notation', () => {
            const float8 = Float8Value.parse('1.5e308');
            expect(float8.value).toBe(1.5e308);
        });

        it('should parse infinity string, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => Float8Value.parse('Infinity')).toThrow();
        });

        it('should parse negative infinity string, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => Float8Value.parse('-Infinity')).toThrow();
        });

        it('should parse NaN string, rejected', () => {
            // the database forbids a non-finite float, so one must never become a value
            expect(() => Float8Value.parse('NaN')).toThrow();
        });

        it('should trim whitespace', () => {
            const float8 = Float8Value.parse('  3.14  ');
            expect(float8.value).toBe(3.14);
        });

        it('should reject an empty string', () => {
            // a non-option value must always be defined, so blank text is not a value
            expect(() => Float8Value.parse('')).toThrow();
        });

        it('should reject a whitespace-only string', () => {
            // a non-option value must always be defined, so blank text is not a value
            expect(() => Float8Value.parse('   ')).toThrow();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Float8Value.parse('abc')).toThrow('Cannot parse "abc" as Float8');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Float8Value.parse('3.14abc')).toThrow('Cannot parse "3.14abc" as Float8');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const float8 = new Float8Value(3.141592653589793);
            expect(float8.valueOf()).toBe(3.141592653589793);
        });

        it('should return zero', () => {
            const float8 = new Float8Value(0);
            expect(float8.valueOf()).toBe(0);
        });
    });
});