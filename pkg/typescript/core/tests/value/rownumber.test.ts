/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {RowNumberValue} from '../../src';

describe('RowNumberValue', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const rowId = new RowNumberValue(BigInt(1000000));
            expect(rowId.value).toBe(BigInt(1000000));
            expect(rowId.type).toBe('RowNumber');
        });

        it('should create instance with valid number value', () => {
            const rowId = new RowNumberValue(1000000);
            expect(rowId.value).toBe(BigInt(1000000));
            expect(rowId.type).toBe('RowNumber');
        });

        it('should truncate decimal number to integer', () => {
            const rowId = new RowNumberValue(42.9);
            expect(rowId.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const rowId = new RowNumberValue(undefined);
            expect(rowId.value).toBeUndefined();
            expect(rowId.type).toBe('RowNumber');
        });

        it('should create instance with no arguments', () => {
            const rowId = new RowNumberValue();
            expect(rowId.value).toBeUndefined();
            expect(rowId.type).toBe('RowNumber');
        });

        it('should accept minimum value 0', () => {
            const rowId = new RowNumberValue(BigInt(0));
            expect(rowId.value).toBe(BigInt(0));
        });

        it('should accept maximum value 18446744073709551615', () => {
            const rowId = new RowNumberValue(BigInt("18446744073709551615"));
            expect(rowId.value).toBe(BigInt("18446744073709551615"));
        });

        it('should accept large positive number', () => {
            const rowId = new RowNumberValue(BigInt("10000000000000000"));
            expect(rowId.value).toBe(BigInt("10000000000000000"));
        });

        it('should throw error for negative value', () => {
            expect(() => new RowNumberValue(BigInt(-1))).toThrow('RowNumber value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new RowNumberValue(BigInt("18446744073709551616"))).toThrow('RowNumber value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const rowId = RowNumberValue.parse('1000000');
            expect(rowId.value).toBe(BigInt(1000000));
        });

        it('should parse minimum value string', () => {
            const rowId = RowNumberValue.parse('0');
            expect(rowId.value).toBe(BigInt(0));
        });

        it('should parse maximum value string', () => {
            const rowId = RowNumberValue.parse('18446744073709551615');
            expect(rowId.value).toBe(BigInt("18446744073709551615"));
        });

        it('should parse large positive number string', () => {
            const rowId = RowNumberValue.parse('10000000000000000');
            expect(rowId.value).toBe(BigInt("10000000000000000"));
        });

        it('should trim whitespace', () => {
            const rowId = RowNumberValue.parse('  1000000  ');
            expect(rowId.value).toBe(BigInt(1000000));
        });

        it('should return undefined for empty string', () => {
            const rowId = RowNumberValue.parse('');
            expect(rowId.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const rowId = RowNumberValue.parse('   ');
            expect(rowId.value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const rowId = RowNumberValue.parse('⟪undefined⟫');
            expect(rowId.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => RowNumberValue.parse('abc')).toThrow('Cannot parse "abc" as RowNumber');
        });

        it('should throw error for negative value string', () => {
            expect(() => RowNumberValue.parse('-1')).toThrow('RowNumber value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => RowNumberValue.parse('18446744073709551616')).toThrow('RowNumber value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });

        it('should throw error for decimal string', () => {
            expect(() => RowNumberValue.parse('42.5')).toThrow('Cannot parse "42.5" as RowNumber');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const rowId = new RowNumberValue(BigInt(1000000));
            expect(rowId.valueOf()).toBe(BigInt(1000000));
        });

        it('should return undefined when value is undefined', () => {
            const rowId = new RowNumberValue(undefined);
            expect(rowId.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const rowId = new RowNumberValue(0);
            expect(rowId.valueOf()).toBe(BigInt(0));
        });
    });

});