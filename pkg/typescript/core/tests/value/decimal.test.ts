// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {DecimalValue} from '../../src';

describe('DecimalValue', () => {
    describe('constructor', () => {
        it('should create instance with valid string value', () => {
            const decimal = new DecimalValue('123.456');
            expect(decimal.value).toBe('123.456');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with integer string', () => {
            const decimal = new DecimalValue('100000');
            expect(decimal.value).toBe('100000');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with negative decimal string', () => {
            const decimal = new DecimalValue('-123.456');
            expect(decimal.value).toBe('-123.456');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with large decimal string', () => {
            const decimal = new DecimalValue('999999999999999999999.123456789');
            expect(decimal.value).toBe('999999999999999999999.123456789');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with small decimal string', () => {
            const decimal = new DecimalValue('0.000000000001');
            expect(decimal.value).toBe('0.000000000001');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with zero', () => {
            const decimal = new DecimalValue('0');
            expect(decimal.value).toBe('0');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with zero decimal', () => {
            const decimal = new DecimalValue('0.0');
            expect(decimal.value).toBe('0.0');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with scientific notation', () => {
            const decimal = new DecimalValue('1.23e-10');
            expect(decimal.value).toBe('1.23e-10');
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with undefined value', () => {
            const decimal = new DecimalValue(undefined);
            expect(decimal.value).toBeUndefined();
            expect(decimal.type).toBe('Decimal');
        });

        it('should create instance with no arguments', () => {
            const decimal = new DecimalValue();
            expect(decimal.value).toBeUndefined();
            expect(decimal.type).toBe('Decimal');
        });

        it('should accept empty string', () => {
            const decimal = new DecimalValue('');
            expect(decimal.value).toBe('');
            expect(decimal.type).toBe('Decimal');
        });

        it('should throw error for non-string value', () => {
            expect(() => new DecimalValue(123 as any)).toThrow('Decimal value must be a string, got number');
        });

        it('should throw error for boolean value', () => {
            expect(() => new DecimalValue(true as any)).toThrow('Decimal value must be a string, got boolean');
        });

        it('should throw error for object value', () => {
            expect(() => new DecimalValue({} as any)).toThrow('Decimal value must be a string, got object');
        });
    });

    describe('parse', () => {
        it('should parse valid decimal string', () => {
            const decimal = DecimalValue.parse('123.456');
            expect(decimal.value).toBe('123.456');
        });

        it('should parse negative decimal string', () => {
            const decimal = DecimalValue.parse('-987.654');
            expect(decimal.value).toBe('-987.654');
        });

        it('should parse integer string', () => {
            const decimal = DecimalValue.parse('42');
            expect(decimal.value).toBe('42');
        });

        it('should parse zero string', () => {
            const decimal = DecimalValue.parse('0');
            expect(decimal.value).toBe('0');
        });

        it('should parse large decimal string', () => {
            const decimal = DecimalValue.parse('999999999999999999999.123456789');
            expect(decimal.value).toBe('999999999999999999999.123456789');
        });

        it('should parse small decimal string', () => {
            const decimal = DecimalValue.parse('0.000000000001');
            expect(decimal.value).toBe('0.000000000001');
        });

        it('should parse scientific notation', () => {
            const decimal = DecimalValue.parse('1.23e-10');
            expect(decimal.value).toBe('1.23e-10');
        });

        it('should parse string with leading zeros', () => {
            const decimal = DecimalValue.parse('00123.456');
            expect(decimal.value).toBe('00123.456');
        });

        it('should parse string with trailing zeros', () => {
            const decimal = DecimalValue.parse('123.45600');
            expect(decimal.value).toBe('123.45600');
        });

        it('should parse empty string', () => {
            const decimal = DecimalValue.parse('');
            expect(decimal.value).toBe('');
        });

        it('should return undefined for undefined marker', () => {
            const decimal = DecimalValue.parse('⟪undefined⟫');
            expect(decimal.value).toBeUndefined();
        });

        it('should parse whitespace string as-is', () => {
            const decimal = DecimalValue.parse('   ');
            expect(decimal.value).toBe('   ');
        });

        it('should parse non-numeric string as-is', () => {
            const decimal = DecimalValue.parse('abc');
            expect(decimal.value).toBe('abc');
        });

        it('should parse mixed string as-is', () => {
            const decimal = DecimalValue.parse('123abc');
            expect(decimal.value).toBe('123abc');
        });
    });

    describe('valueOf', () => {
        it('should return the string value', () => {
            const decimal = new DecimalValue('123.456');
            expect(decimal.valueOf()).toBe('123.456');
        });

        it('should return undefined when value is undefined', () => {
            const decimal = new DecimalValue(undefined);
            expect(decimal.valueOf()).toBeUndefined();
        });

        it('should return negative value', () => {
            const decimal = new DecimalValue('-123.456');
            expect(decimal.valueOf()).toBe('-123.456');
        });

        it('should return zero', () => {
            const decimal = new DecimalValue('0');
            expect(decimal.valueOf()).toBe('0');
        });

        it('should return empty string', () => {
            const decimal = new DecimalValue('');
            expect(decimal.valueOf()).toBe('');
        });

        it('should return large decimal', () => {
            const decimal = new DecimalValue('999999999999999999999.123456789');
            expect(decimal.valueOf()).toBe('999999999999999999999.123456789');
        });
    });

    describe('toString', () => {
        it('should return string representation of value', () => {
            const decimal = new DecimalValue('123.456');
            expect(decimal.toString()).toBe('123.456');
        });

        it('should return "undefined" when value is undefined', () => {
            const decimal = new DecimalValue(undefined);
            expect(decimal.toString()).toBe('undefined');
        });

        it('should return negative value as string', () => {
            const decimal = new DecimalValue('-123.456');
            expect(decimal.toString()).toBe('-123.456');
        });

        it('should return zero as string', () => {
            const decimal = new DecimalValue('0');
            expect(decimal.toString()).toBe('0');
        });

        it('should return empty string', () => {
            const decimal = new DecimalValue('');
            expect(decimal.toString()).toBe('');
        });
    });

    describe('equals', () => {
        it('should return true for equal decimal values', () => {
            const decimal1 = new DecimalValue('123.456');
            const decimal2 = new DecimalValue('123.456');
            expect(decimal1.equals(decimal2)).toBe(true);
        });

        it('should return false for different decimal values', () => {
            const decimal1 = new DecimalValue('123.456');
            const decimal2 = new DecimalValue('123.457');
            expect(decimal1.equals(decimal2)).toBe(false);
        });

        it('should return true for both undefined', () => {
            const decimal1 = new DecimalValue(undefined);
            const decimal2 = new DecimalValue(undefined);
            expect(decimal1.equals(decimal2)).toBe(true);
        });

        it('should return false when one is undefined', () => {
            const decimal1 = new DecimalValue('123.456');
            const decimal2 = new DecimalValue(undefined);
            expect(decimal1.equals(decimal2)).toBe(false);
        });

        it('should return false for different types', () => {
            const decimal = new DecimalValue('123.456');
            const utf8 = {type: 'Utf8', value: '123.456', equals: () => false} as any;
            expect(decimal.equals(utf8)).toBe(false);
        });

        it('should return true for zero values', () => {
            const decimal1 = new DecimalValue('0');
            const decimal2 = new DecimalValue('0');
            expect(decimal1.equals(decimal2)).toBe(true);
        });

        it('should return false for different zero representations', () => {
            const decimal1 = new DecimalValue('0');
            const decimal2 = new DecimalValue('0.0');
            expect(decimal1.equals(decimal2)).toBe(false);
        });

        it('should return true for negative values', () => {
            const decimal1 = new DecimalValue('-123.456');
            const decimal2 = new DecimalValue('-123.456');
            expect(decimal1.equals(decimal2)).toBe(true);
        });

        it('should return true for large decimals', () => {
            const decimal1 = new DecimalValue('999999999999999999999.123456789');
            const decimal2 = new DecimalValue('999999999999999999999.123456789');
            expect(decimal1.equals(decimal2)).toBe(true);
        });
    });

    describe('encode', () => {
        it('should encode decimal value to TypeValuePair', () => {
            const decimal = new DecimalValue('123.456');
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: '123.456'
            });
        });

        it('should encode undefined to undefined marker', () => {
            const decimal = new DecimalValue(undefined);
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: '⟪undefined⟫'
            });
        });

        it('should encode negative value', () => {
            const decimal = new DecimalValue('-123.456');
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: '-123.456'
            });
        });

        it('should encode zero', () => {
            const decimal = new DecimalValue('0');
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: '0'
            });
        });

        it('should encode empty string', () => {
            const decimal = new DecimalValue('');
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: ''
            });
        });

        it('should encode large decimal', () => {
            const decimal = new DecimalValue('999999999999999999999.123456789');
            const encoded = decimal.encode();
            expect(encoded).toEqual({
                type: 'Decimal',
                value: '999999999999999999999.123456789'
            });
        });
    });

    describe('round-trip', () => {
        it('should handle encode/parse round-trip', () => {
            const original = new DecimalValue('123.456');
            const encoded = original.encode();
            const parsed = DecimalValue.parse(encoded.value);
            expect(parsed.value).toBe(original.value);
            expect(parsed.equals(original)).toBe(true);
        });

        it('should handle round-trip with undefined', () => {
            const original = new DecimalValue(undefined);
            const encoded = original.encode();
            const parsed = DecimalValue.parse(encoded.value);
            expect(parsed.value).toBe(original.value);
            expect(parsed.equals(original)).toBe(true);
        });

        it('should handle round-trip with negative value', () => {
            const original = new DecimalValue('-987.654');
            const encoded = original.encode();
            const parsed = DecimalValue.parse(encoded.value);
            expect(parsed.value).toBe(original.value);
            expect(parsed.equals(original)).toBe(true);
        });

        it('should handle round-trip with large decimal', () => {
            const original = new DecimalValue('999999999999999999999.123456789');
            const encoded = original.encode();
            const parsed = DecimalValue.parse(encoded.value);
            expect(parsed.value).toBe(original.value);
            expect(parsed.equals(original)).toBe(true);
        });
    });
});
