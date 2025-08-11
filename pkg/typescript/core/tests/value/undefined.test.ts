/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { UndefinedValue } from '../../src';

describe('UndefinedValue', () => {
    describe('constructor', () => {
        it('should create instance', () => {
            const value = new UndefinedValue();
            expect(value.value).toBeUndefined();
            expect(value.type).toBe('Undefined');
        });
    });

    describe('new', () => {
        it('should create new undefined value', () => {
            const value = UndefinedValue.new();
            expect(value.value).toBeUndefined();
        });
    });

    describe('default', () => {
        it('should return undefined value as default', () => {
            const value = UndefinedValue.default();
            expect(value.value).toBeUndefined();
        });
    });

    describe('parse', () => {
        it('should parse empty string as undefined', () => {
            const value = UndefinedValue.parse('');
            expect(value.value).toBeUndefined();
        });

        it('should parse whitespace as undefined', () => {
            const value = UndefinedValue.parse('   ');
            expect(value.value).toBeUndefined();
        });

        it('should parse "undefined" string as undefined', () => {
            const value = UndefinedValue.parse('undefined');
            expect(value.value).toBeUndefined();
        });

        it('should parse UNDEFINED_VALUE constant', () => {
            const value = UndefinedValue.parse('⟪undefined⟫');
            expect(value.value).toBeUndefined();
        });

        it('should throw error for non-undefined string', () => {
            expect(() => UndefinedValue.parse('hello')).toThrow('Cannot parse');
            expect(() => UndefinedValue.parse('123')).toThrow('Cannot parse');
            expect(() => UndefinedValue.parse('null')).toThrow('Cannot parse');
        });
    });

    describe('isUndefined', () => {
        it('should always return true', () => {
            const value = new UndefinedValue();
            expect(value.isUndefined()).toBe(true);
        });
    });

    describe('toString', () => {
        it('should format as "undefined"', () => {
            const value = new UndefinedValue();
            expect(value.toString()).toBe('undefined');
        });
    });

    describe('valueOf', () => {
        it('should return undefined', () => {
            const value = new UndefinedValue();
            expect(value.valueOf()).toBeUndefined();
        });
    });

    describe('equals', () => {
        it('should always be equal to another UndefinedValue', () => {
            const value1 = new UndefinedValue();
            const value2 = new UndefinedValue();
            expect(value1.equals(value2)).toBe(true);
        });
    });

    describe('compare', () => {
        it('should always return 0 (equal)', () => {
            const value1 = new UndefinedValue();
            const value2 = new UndefinedValue();
            expect(value1.compare(value2)).toBe(0);
        });
    });
});