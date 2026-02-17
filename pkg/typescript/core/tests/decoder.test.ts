// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { decode } from '../src/decoder';
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue,
    Float4Value, Float8Value,
    Int4Value, DurationValue, TimeValue,
    NoneValue, Utf8Value, Uuid4Value, Uuid7Value, IdentityIdValue
} from '../src/value';
import { NONE_VALUE } from '../src/constant';

describe('decode', () => {
    it('should decode Boolean type with "true" value', () => {
        const pair = { type: 'Boolean' as const, value: 'true' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(BooleanValue);
        expect(result.type).toBe('Boolean');
        expect(result.valueOf()).toBe(true);
    });

    it('should decode Boolean type with "false" value', () => {
        const pair = { type: 'Boolean' as const, value: 'false' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(BooleanValue);
        expect(result.type).toBe('Boolean');
        expect(result.valueOf()).toBe(false);
    });

    it('should decode Boolean type with empty value', () => {
        const pair = { type: 'Boolean' as const, value: '' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(BooleanValue);
        expect(result.type).toBe('Boolean');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should decode Int4 type with positive number', () => {
        const pair = { type: 'Int4' as const, value: '42' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBe(42);
    });

    it('should decode Int4 type with negative number', () => {
        const pair = { type: 'Int4' as const, value: '-123' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBe(-123);
    });

    it('should decode Int4 type with empty value', () => {
        const pair = { type: 'Int4' as const, value: '' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should decode Utf8 type with string value', () => {
        const pair = { type: 'Utf8' as const, value: 'hello world' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(Utf8Value);
        expect(result.type).toBe('Utf8');
        expect(result.valueOf()).toBe('hello world');
    });

    it('should decode Utf8 type with empty string value', () => {
        const pair = { type: 'Utf8' as const, value: '' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(Utf8Value);
        expect(result.type).toBe('Utf8');
        expect(result.valueOf()).toBe('');
    });

    it('should decode None type', () => {
        const pair = { type: 'None' as const, value: '' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(NoneValue);
        expect(result.type).toBe('None');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should throw error for unsupported type', () => {
        const pair = { type: 'InvalidType' as any, value: 'test' };

        expect(() => decode(pair)).toThrow('Unsupported type: InvalidType');
    });

    it('should handle round-trip encoding/decoding for Boolean', () => {
        const original = new BooleanValue(true);
        const encoded = original.encode();
        const decoded = decode(encoded);

        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    it('should handle round-trip encoding/decoding for Int4', () => {
        const original = new Int4Value(42);
        const encoded = original.encode();
        const decoded = decode(encoded);

        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    it('should handle round-trip encoding/decoding for Utf8', () => {
        const original = new Utf8Value('hello world');
        const encoded = original.encode();
        const decoded = decode(encoded);

        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    it('should decode Decimal type with positive decimal value', () => {
        const pair = { type: 'Decimal' as const, value: '123.456' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.type).toBe('Decimal');
        expect(result.valueOf()).toBe('123.456');
    });

    it('should decode Decimal type with negative decimal value', () => {
        const pair = { type: 'Decimal' as const, value: '-987.654' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.type).toBe('Decimal');
        expect(result.valueOf()).toBe('-987.654');
    });

    it('should decode Decimal type with integer value', () => {
        const pair = { type: 'Decimal' as const, value: '42' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.type).toBe('Decimal');
        expect(result.valueOf()).toBe('42');
    });

    it('should decode Decimal type with large decimal value', () => {
        const pair = { type: 'Decimal' as const, value: '999999999999999999999.123456789' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.type).toBe('Decimal');
        expect(result.valueOf()).toBe('999999999999999999999.123456789');
    });

    it('should decode Decimal type with empty value', () => {
        const pair = { type: 'Decimal' as const, value: '' };
        const result = decode(pair);

        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.type).toBe('Decimal');
        expect(result.valueOf()).toBe('');
    });

    it('should handle round-trip encoding/decoding for Decimal', () => {
        const original = new DecimalValue('123.456');
        const encoded = original.encode();
        const decoded = decode(encoded);

        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    describe('Option type decoding', () => {
        it('should decode Option<Int4> with value', () => {
            const pair = { type: { Option: 'Int4' as const }, value: '42' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Int4Value);
            expect(result.type).toBe('Int4');
            expect(result.valueOf()).toBe(42);
        });

        it('should decode Option<Int4> with none value', () => {
            const pair = { type: { Option: 'Int4' as const }, value: '⟪none⟫' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect(result.valueOf()).toBeUndefined();
        });

        it('should decode Option<Int4> with empty value as none', () => {
            const pair = { type: { Option: 'Int4' as const }, value: '' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect(result.valueOf()).toBeUndefined();
        });

        it('should decode Option<Boolean> with true value', () => {
            const pair = { type: { Option: 'Boolean' as const }, value: 'true' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(BooleanValue);
            expect(result.valueOf()).toBe(true);
        });

        it('should decode Option<Utf8> with none value', () => {
            const pair = { type: { Option: 'Utf8' as const }, value: '⟪none⟫' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect(result.valueOf()).toBeUndefined();
        });

        it('should decode Option<Utf8> with string value', () => {
            const pair = { type: { Option: 'Utf8' as const }, value: 'hello' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Utf8Value);
            expect(result.valueOf()).toBe('hello');
        });

        it('should decode Option<Int4> none with correct innerType', () => {
            const pair = { type: { Option: 'Int4' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Int4');
        });

        it('should decode Option<Date> with value', () => {
            const pair = { type: { Option: 'Date' as const }, value: '2024-03-15' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(DateValue);
            expect((result as DateValue).toString()).toBe('2024-03-15');
        });

        it('should decode Option<Date> with none value', () => {
            const pair = { type: { Option: 'Date' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Date');
        });

        it('should decode Option<Date> with empty value as none', () => {
            const pair = { type: { Option: 'Date' as const }, value: '' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Date');
        });

        it('should decode Option<DateTime> with value', () => {
            const pair = { type: { Option: 'DateTime' as const }, value: '2024-03-15T10:30:00Z' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(DateTimeValue);
        });

        it('should decode Option<DateTime> with none value', () => {
            const pair = { type: { Option: 'DateTime' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('DateTime');
        });

        it('should decode Option<Time> with value', () => {
            const pair = { type: { Option: 'Time' as const }, value: '10:30:00' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(TimeValue);
        });

        it('should decode Option<Time> with none value', () => {
            const pair = { type: { Option: 'Time' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Time');
        });

        it('should decode Option<Duration> with value', () => {
            const pair = { type: { Option: 'Duration' as const }, value: 'PT1H' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(DurationValue);
        });

        it('should decode Option<Duration> with none value', () => {
            const pair = { type: { Option: 'Duration' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Duration');
        });

        it('should decode Option<Blob> with value', () => {
            const pair = { type: { Option: 'Blob' as const }, value: '0x0102' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(BlobValue);
        });

        it('should decode Option<Blob> with none value', () => {
            const pair = { type: { Option: 'Blob' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Blob');
        });

        it('should decode Option<Decimal> with value', () => {
            const pair = { type: { Option: 'Decimal' as const }, value: '123.456' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(DecimalValue);
            expect(result.valueOf()).toBe('123.456');
        });

        it('should decode Option<Decimal> with none value', () => {
            const pair = { type: { Option: 'Decimal' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Decimal');
        });

        it('should decode Option<Uuid4> with value', () => {
            const pair = { type: { Option: 'Uuid4' as const }, value: '550e8400-e29b-41d4-a716-446655440000' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Uuid4Value);
            expect(result.valueOf()).toBe('550e8400-e29b-41d4-a716-446655440000');
        });

        it('should decode Option<Uuid4> with none value', () => {
            const pair = { type: { Option: 'Uuid4' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Uuid4');
        });

        it('should decode Option<Uuid7> with value', () => {
            const pair = { type: { Option: 'Uuid7' as const }, value: '01932c07-a000-7000-8000-000000000000' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Uuid7Value);
        });

        it('should decode Option<Uuid7> with none value', () => {
            const pair = { type: { Option: 'Uuid7' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Uuid7');
        });

        it('should decode Option<IdentityId> with none value', () => {
            const pair = { type: { Option: 'IdentityId' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('IdentityId');
        });

        it('should decode Option<Float4> with value', () => {
            const pair = { type: { Option: 'Float4' as const }, value: '3.14' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Float4Value);
            expect(result.valueOf()).toBeCloseTo(3.14);
        });

        it('should decode Option<Float4> with none value', () => {
            const pair = { type: { Option: 'Float4' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Float4');
        });

        it('should decode Option<Float8> with value', () => {
            const pair = { type: { Option: 'Float8' as const }, value: '3.141592653589793' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(Float8Value);
            expect(result.valueOf()).toBeCloseTo(3.141592653589793);
        });

        it('should decode Option<Float8> with none value', () => {
            const pair = { type: { Option: 'Float8' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Float8');
        });

        it('should decode Option<Boolean> with none value and preserve innerType', () => {
            const pair = { type: { Option: 'Boolean' as const }, value: NONE_VALUE };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Boolean');
        });

        it('should decode Option<Utf8> with empty value and preserve innerType', () => {
            const pair = { type: { Option: 'Utf8' as const }, value: '' };
            const result = decode(pair);

            expect(result).toBeInstanceOf(NoneValue);
            expect((result as NoneValue).innerType).toBe('Utf8');
        });
    });
});
