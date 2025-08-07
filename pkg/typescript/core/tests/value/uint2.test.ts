/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Uint2} from '../../src/value/uint2';

describe('Uint2', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const uint2 = new Uint2(30000);
            expect(uint2.value).toBe(30000);
            expect(uint2.type).toBe('Uint2');
        });

        it('should create instance with undefined value', () => {
            const uint2 = new Uint2(undefined);
            expect(uint2.value).toBeUndefined();
            expect(uint2.type).toBe('Uint2');
        });

        it('should create instance with no arguments', () => {
            const uint2 = new Uint2();
            expect(uint2.value).toBeUndefined();
            expect(uint2.type).toBe('Uint2');
        });

        it('should accept minimum value 0', () => {
            const uint2 = new Uint2(0);
            expect(uint2.value).toBe(0);
        });

        it('should accept maximum value 65535', () => {
            const uint2 = new Uint2(65535);
            expect(uint2.value).toBe(65535);
        });

        it('should throw error for negative value', () => {
            expect(() => new Uint2(-1)).toThrow('Uint2 value must be between 0 and 65535, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Uint2(65536)).toThrow('Uint2 value must be between 0 and 65535, got 65536');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Uint2(42.5)).toThrow('Uint2 value must be an integer, got 42.5');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const uint2 = Uint2.parse('30000');
            expect(uint2.value).toBe(30000);
        });

        it('should parse minimum value string', () => {
            const uint2 = Uint2.parse('0');
            expect(uint2.value).toBe(0);
        });

        it('should parse maximum value string', () => {
            const uint2 = Uint2.parse('65535');
            expect(uint2.value).toBe(65535);
        });

        it('should trim whitespace', () => {
            const uint2 = Uint2.parse('  30000  ');
            expect(uint2.value).toBe(30000);
        });

        it('should return undefined for empty string', () => {
            const uint2 = Uint2.parse('');
            expect(uint2.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const uint2 = Uint2.parse('   ');
            expect(uint2.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Uint2.parse('abc')).toThrow('Cannot parse "abc" as Uint2');
        });

        it('should throw error for negative value string', () => {
            expect(() => Uint2.parse('-1')).toThrow('Uint2 value must be between 0 and 65535, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Uint2.parse('65536')).toThrow('Uint2 value must be between 0 and 65535, got 65536');
        });

        it('should throw error for decimal string', () => {
            expect(() => Uint2.parse('42.5')).toThrow('Uint2 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const uint2 = new Uint2(30000);
            expect(uint2.valueOf()).toBe(30000);
        });

        it('should return undefined when value is undefined', () => {
            const uint2 = new Uint2(undefined);
            expect(uint2.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const uint2 = new Uint2(0);
            expect(uint2.valueOf()).toBe(0);
        });
    });
});