// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { NoneValue } from '../../src';

describe('NoneValue', () => {
    describe('constructor', () => {
        it('should create instance with default innerType', () => {
            const value = new NoneValue();
            expect(value.value).toBeUndefined();
            expect(value.type).toBe('None');
            expect(value.innerType).toBe('None');
        });

        it('should create instance with specified innerType', () => {
            const value = new NoneValue('Int4');
            expect(value.value).toBeUndefined();
            expect(value.type).toBe('None');
            expect(value.innerType).toBe('Int4');
        });
    });

    describe('parse', () => {
        it('should parse empty string as none', () => {
            const value = NoneValue.parse('');
            expect(value.value).toBeUndefined();
        });

        it('should parse whitespace as none', () => {
            const value = NoneValue.parse('   ');
            expect(value.value).toBeUndefined();
        });

        it('should parse "none" string as none', () => {
            const value = NoneValue.parse('none');
            expect(value.value).toBeUndefined();
        });

        it('should parse NONE_VALUE constant', () => {
            const value = NoneValue.parse('⟪none⟫');
            expect(value.value).toBeUndefined();
        });

        it('should accept innerType parameter', () => {
            const value = NoneValue.parse('⟪none⟫', 'Int4');
            expect(value.innerType).toBe('Int4');
        });

        it('should throw error for non-none string', () => {
            expect(() => NoneValue.parse('hello')).toThrow('Cannot parse');
            expect(() => NoneValue.parse('123')).toThrow('Cannot parse');
            expect(() => NoneValue.parse('null')).toThrow('Cannot parse');
        });
    });

    describe('isNone', () => {
        it('should always return true', () => {
            const value = new NoneValue();
            expect(value.isNone()).toBe(true);
        });
    });

    describe('toString', () => {
        it('should format as "none"', () => {
            const value = new NoneValue();
            expect(value.toString()).toBe('none');
        });
    });

    describe('valueOf', () => {
        it('should return undefined', () => {
            const value = new NoneValue();
            expect(value.valueOf()).toBeUndefined();
        });
    });

    describe('equals', () => {
        it('should be equal to another NoneValue', () => {
            const value1 = new NoneValue();
            const value2 = new NoneValue();
            expect(value1.equals(value2)).toBe(true);
        });

        it('should be equal to NoneValue with different innerType', () => {
            const value1 = new NoneValue('Int4');
            const value2 = new NoneValue('Utf8');
            expect(value1.equals(value2)).toBe(true);
        });
    });

    describe('compare', () => {
        it('should always return 0 (equal)', () => {
            const value1 = new NoneValue();
            const value2 = new NoneValue();
            expect(value1.compare(value2)).toBe(0);
        });
    });

    describe('encode', () => {
        it('should encode as None type with NONE_VALUE', () => {
            const value = new NoneValue();
            const encoded = value.encode();
            expect(encoded.type).toBe('None');
            expect(encoded.value).toBe('⟪none⟫');
        });
    });
});
