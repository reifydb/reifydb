/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {RowIdValue} from '../../src';

describe('RowIdValue', () => {
    describe('constructor', () => {
        it('should create instance with valid bigint value', () => {
            const rowId = new RowIdValue(BigInt(1000000));
            expect(rowId.value).toBe(BigInt(1000000));
            expect(rowId.type).toBe('RowId');
        });

        it('should create instance with valid number value', () => {
            const rowId = new RowIdValue(1000000);
            expect(rowId.value).toBe(BigInt(1000000));
            expect(rowId.type).toBe('RowId');
        });

        it('should truncate decimal number to integer', () => {
            const rowId = new RowIdValue(42.9);
            expect(rowId.value).toBe(BigInt(42));
        });

        it('should create instance with undefined value', () => {
            const rowId = new RowIdValue(undefined);
            expect(rowId.value).toBeUndefined();
            expect(rowId.type).toBe('RowId');
        });

        it('should create instance with no arguments', () => {
            const rowId = new RowIdValue();
            expect(rowId.value).toBeUndefined();
            expect(rowId.type).toBe('RowId');
        });

        it('should accept minimum value 0', () => {
            const rowId = new RowIdValue(BigInt(0));
            expect(rowId.value).toBe(BigInt(0));
        });

        it('should accept maximum value 18446744073709551615', () => {
            const rowId = new RowIdValue(BigInt("18446744073709551615"));
            expect(rowId.value).toBe(BigInt("18446744073709551615"));
        });

        it('should accept large positive number', () => {
            const rowId = new RowIdValue(BigInt("10000000000000000"));
            expect(rowId.value).toBe(BigInt("10000000000000000"));
        });

        it('should throw error for negative value', () => {
            expect(() => new RowIdValue(BigInt(-1))).toThrow('RowId value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value above maximum', () => {
            expect(() => new RowIdValue(BigInt("18446744073709551616"))).toThrow('RowId value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });
    });

    describe('parse', () => {
        it('should parse valid integer string', () => {
            const rowId = RowIdValue.parse('1000000');
            expect(rowId.value).toBe(BigInt(1000000));
        });

        it('should parse minimum value string', () => {
            const rowId = RowIdValue.parse('0');
            expect(rowId.value).toBe(BigInt(0));
        });

        it('should parse maximum value string', () => {
            const rowId = RowIdValue.parse('18446744073709551615');
            expect(rowId.value).toBe(BigInt("18446744073709551615"));
        });

        it('should parse large positive number string', () => {
            const rowId = RowIdValue.parse('10000000000000000');
            expect(rowId.value).toBe(BigInt("10000000000000000"));
        });

        it('should trim whitespace', () => {
            const rowId = RowIdValue.parse('  1000000  ');
            expect(rowId.value).toBe(BigInt(1000000));
        });

        it('should return undefined for empty string', () => {
            const rowId = RowIdValue.parse('');
            expect(rowId.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const rowId = RowIdValue.parse('   ');
            expect(rowId.value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const rowId = RowIdValue.parse('⟪undefined⟫');
            expect(rowId.value).toBeUndefined();
        });

        it('should throw error for non-numeric string', () => {
            expect(() => RowIdValue.parse('abc')).toThrow('Cannot parse "abc" as RowId');
        });

        it('should throw error for negative value string', () => {
            expect(() => RowIdValue.parse('-1')).toThrow('RowId value must be between 0 and 18446744073709551615, got -1');
        });

        it('should throw error for value outside range', () => {
            expect(() => RowIdValue.parse('18446744073709551616')).toThrow('RowId value must be between 0 and 18446744073709551615, got 18446744073709551616');
        });

        it('should throw error for decimal string', () => {
            expect(() => RowIdValue.parse('42.5')).toThrow('Cannot parse "42.5" as RowId');
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const rowId = new RowIdValue(BigInt(1000000));
            expect(rowId.valueOf()).toBe(BigInt(1000000));
        });

        it('should return undefined when value is undefined', () => {
            const rowId = new RowIdValue(undefined);
            expect(rowId.valueOf()).toBeUndefined();
        });

        it('should return zero', () => {
            const rowId = new RowIdValue(0);
            expect(rowId.valueOf()).toBe(BigInt(0));
        });
    });

});