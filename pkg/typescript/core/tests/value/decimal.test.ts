// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {DecimalValue, NoneValue, decode} from '../../src';

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

        it('should reject undefined', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new DecimalValue(undefined)).toThrow();
        });

        it('should reject no arguments', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new DecimalValue()).toThrow();
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

        it('should reject an empty string', () => {
            // a non-option Decimal must always be defined, so blank text is not a value
            expect(() => DecimalValue.parse('')).toThrow();
        });

        it('should reject the none marker', () => {
            // a non-option Decimal must always be defined, so the marker is not a value
            expect(() => DecimalValue.parse('⟪none⟫')).toThrow();
        });

        it('should reject a whitespace string', () => {
            // blank text is not a number and must not be carried through as the value
            expect(() => DecimalValue.parse('   ')).toThrow();
        });

        it('should reject a non-numeric string', () => {
            // an unparsable Decimal must fail loud instead of being carried through as text
            expect(() => DecimalValue.parse('abc')).toThrow();
        });

        it('should reject a mixed string', () => {
            // a trailing tail makes the number ambiguous, so it must not parse as its numeric prefix
            expect(() => DecimalValue.parse('123abc')).toThrow();
        });
    });

    describe('valueOf', () => {
        it('should return the string value', () => {
            const decimal = new DecimalValue('123.456');
            expect(decimal.valueOf()).toBe('123.456');
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

        it('should round-trip a none through NoneValue', () => {
            // a none travels as an option type, so a bare Decimal can never carry the marker
            const encoded = new NoneValue('Decimal').encode();
            expect(encoded.type).toEqual({Option: 'Decimal'});
            expect(decode(encoded)).toBeInstanceOf(NoneValue);
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
