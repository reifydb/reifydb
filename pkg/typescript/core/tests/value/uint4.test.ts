/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Uint4Value} from '../../src/value/uint4';

describe('Uint4Value', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const uint4 = new Uint4Value(1000000);
            expect(uint4.value).toBe(1000000);
            expect(uint4.type).toBe('Uint4');
        });

        it('should create instance with undefined value', () => {
            const uint4 = new Uint4Value(undefined);
            expect(uint4.value).toBeUndefined();
            expect(uint4.type).toBe('Uint4');
        });

        it('should create instance with no arguments', () => {
            const uint4 = new Uint4Value();
            expect(uint4.value).toBeUndefined();
            expect(uint4.type).toBe('Uint4');
        });

        it('should accept minimum value 0', () => {
            const uint4 = new Uint4Value(0);
            expect(uint4.value).toBe(0);
        });

        it('should accept maximum value 4294967295', () => {
            const uint4 = new Uint4Value(4294967295);
            expect(uint4.value).toBe(4294967295);
        });

        it('should throw error for negative value', () => {
            expect(() => new Uint4Value(-1)).toThrow('Uint4 value must be between 0 and 4294967295, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Uint4Value(4294967296)).toThrow('Uint4 value must be between 0 and 4294967295, got 4294967296');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Uint4Value(42.5)).toThrow('Uint4 value must be an integer, got 42.5');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const uint4 = Uint4Value.parse('1000000');
            expect(uint4.value).toBe(1000000);
        });

        it('should parse minimum value string', () => {
            const uint4 = Uint4Value.parse('0');
            expect(uint4.value).toBe(0);
        });

        it('should parse maximum value string', () => {
            const uint4 = Uint4Value.parse('4294967295');
            expect(uint4.value).toBe(4294967295);
        });

        it('should trim whitespace', () => {
            const uint4 = Uint4Value.parse('  1000000  ');
            expect(uint4.value).toBe(1000000);
        });

        it('should return undefined for empty string', () => {
            const uint4 = Uint4Value.parse('');
            expect(uint4.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const uint4 = Uint4Value.parse('   ');
            expect(uint4.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Uint4Value.parse('abc')).toThrow('Cannot parse "abc" as Uint4');
        });

        it('should throw error for negative value string', () => {
            expect(() => Uint4Value.parse('-1')).toThrow('Uint4 value must be between 0 and 4294967295, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Uint4Value.parse('4294967296')).toThrow('Uint4 value must be between 0 and 4294967295, got 4294967296');
        });

        it('should throw error for decimal string', () => {
            expect(() => Uint4Value.parse('42.5')).toThrow('Uint4 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const uint4 = new Uint4Value(1000000);
            expect(uint4.valueOf()).toBe(1000000);
        });

        it('should return undefined when value is undefined', () => {
            const uint4 = new Uint4Value(undefined);
            expect(uint4.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const uint4 = new Uint4Value(0);
            expect(uint4.valueOf()).toBe(0);
        });
    });
});