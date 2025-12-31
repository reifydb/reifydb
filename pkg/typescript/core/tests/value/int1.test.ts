// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {Int1Value} from '../../src';

describe('Int1Value', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const int1 = new Int1Value(42);
            expect(int1.value).toBe(42);
            expect(int1.type).toBe('Int1');
        });

        it('should create instance with undefined value', () => {
            const int1 = new Int1Value(undefined);
            expect(int1.value).toBeUndefined();
            expect(int1.type).toBe('Int1');
        });

        it('should create instance with no arguments', () => {
            const int1 = new Int1Value();
            expect(int1.value).toBeUndefined();
            expect(int1.type).toBe('Int1');
        });

        it('should accept minimum value -128', () => {
            const int1 = new Int1Value(-128);
            expect(int1.value).toBe(-128);
        });

        it('should accept maximum value 127', () => {
            const int1 = new Int1Value(127);
            expect(int1.value).toBe(127);
        });

        it('should accept zero', () => {
            const int1 = new Int1Value(0);
            expect(int1.value).toBe(0);
        });

        it('should throw error for value below minimum', () => {
            expect(() => new Int1Value(-129)).toThrow('Int1 value must be between -128 and 127, got -129');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Int1Value(128)).toThrow('Int1 value must be between -128 and 127, got 128');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Int1Value(42.5)).toThrow('Int1 value must be an integer, got 42.5');
        });

        it('should throw error for decimal close to integer', () => {
            expect(() => new Int1Value(42.001)).toThrow('Int1 value must be an integer, got 42.001');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const int1 = Int1Value.parse('42');
            expect(int1.value).toBe(42);
        });

        it('should parse negative integer string', () => {
            const int1 = Int1Value.parse('-42');
            expect(int1.value).toBe(-42);
        });

        it('should parse minimum value string', () => {
            const int1 = Int1Value.parse('-128');
            expect(int1.value).toBe(-128);
        });

        it('should parse maximum value string', () => {
            const int1 = Int1Value.parse('127');
            expect(int1.value).toBe(127);
        });

        it('should parse zero string', () => {
            const int1 = Int1Value.parse('0');
            expect(int1.value).toBe(0);
        });

        it('should trim whitespace', () => {
            const int1 = Int1Value.parse('  42  ');
            expect(int1.value).toBe(42);
        });

        it('should return undefined for empty string', () => {
            const int1 = Int1Value.parse('');
            expect(int1.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const int1 = Int1Value.parse('   ');
            expect(int1.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Int1Value.parse('abc')).toThrow('Cannot parse "abc" as Int1');
        });

        it('should throw error for mixed alphanumeric', () => {
            expect(() => Int1Value.parse('42abc')).toThrow('Cannot parse "42abc" as Int1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Int1Value.parse('128')).toThrow('Int1 value must be between -128 and 127, got 128');
        });

        it('should throw error for decimal string', () => {
            expect(() => Int1Value.parse('42.5')).toThrow('Int1 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const int1 = new Int1Value(42);
            expect(int1.valueOf()).toBe(42);
        });

        it('should return undefined when value is undefined', () => {
            const int1 = new Int1Value(undefined);
            expect(int1.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const int1 = new Int1Value(-42);
            expect(int1.valueOf()).toBe(-42);
        });

        it('should return zero', () => {
            const int1 = new Int1Value(0);
            expect(int1.valueOf()).toBe(0);
        });
    });
});