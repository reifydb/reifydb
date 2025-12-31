// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {Uint1Value} from '../../src';

describe('Uint1Value', () => {
    describe('constructor', () => {
        it('should create instance with valid value', () => {
            const uint1 = new Uint1Value(100);
            expect(uint1.value).toBe(100);
            expect(uint1.type).toBe('Uint1');
        });

        it('should create instance with undefined value', () => {
            const uint1 = new Uint1Value(undefined);
            expect(uint1.value).toBeUndefined();
            expect(uint1.type).toBe('Uint1');
        });

        it('should create instance with no arguments', () => {
            const uint1 = new Uint1Value();
            expect(uint1.value).toBeUndefined();
            expect(uint1.type).toBe('Uint1');
        });

        it('should accept minimum value 0', () => {
            const uint1 = new Uint1Value(0);
            expect(uint1.value).toBe(0);
        });

        it('should accept maximum value 255', () => {
            const uint1 = new Uint1Value(255);
            expect(uint1.value).toBe(255);
        });

        it('should throw error for negative value', () => {
            expect(() => new Uint1Value(-1)).toThrow('Uint1 value must be between 0 and 255, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new Uint1Value(256)).toThrow('Uint1 value must be between 0 and 255, got 256');
        });

        it('should throw error for non-integer value', () => {
            expect(() => new Uint1Value(42.5)).toThrow('Uint1 value must be an integer, got 42.5');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const uint1 = Uint1Value.parse('100');
            expect(uint1.value).toBe(100);
        });

        it('should parse minimum value string', () => {
            const uint1 = Uint1Value.parse('0');
            expect(uint1.value).toBe(0);
        });

        it('should parse maximum value string', () => {
            const uint1 = Uint1Value.parse('255');
            expect(uint1.value).toBe(255);
        });

        it('should trim whitespace', () => {
            const uint1 = Uint1Value.parse('  100  ');
            expect(uint1.value).toBe(100);
        });

        it('should return undefined for empty string', () => {
            const uint1 = Uint1Value.parse('');
            expect(uint1.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const uint1 = Uint1Value.parse('   ');
            expect(uint1.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => Uint1Value.parse('abc')).toThrow('Cannot parse "abc" as Uint1');
        });

        it('should throw error for negative value string', () => {
            expect(() => Uint1Value.parse('-1')).toThrow('Uint1 value must be between 0 and 255, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => Uint1Value.parse('256')).toThrow('Uint1 value must be between 0 and 255, got 256');
        });

        it('should throw error for decimal string', () => {
            expect(() => Uint1Value.parse('42.5')).toThrow('Uint1 value must be an integer, got 42.5');
        });
    });

    describe('valueOf', () => {
        it('should return the numeric value', () => {
            const uint1 = new Uint1Value(100);
            expect(uint1.valueOf()).toBe(100);
        });

        it('should return undefined when value is undefined', () => {
            const uint1 = new Uint1Value(undefined);
            expect(uint1.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const uint1 = new Uint1Value(0);
            expect(uint1.valueOf()).toBe(0);
        });
    });
});