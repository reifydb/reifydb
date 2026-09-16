// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue, Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, DurationValue,
    TimeValue, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    Uint16Value, NoneValue, Utf8Value, Uuid4Value, Uuid7Value, IdentityIdValue, decode
} from '../../src';
import {NONE_VALUE} from '../../src/constant';

describe('Value encode method', () => {
    describe('BooleanValue', () => {
        it('should encode true and be parseable', () => {
            const value = new BooleanValue(true);
            const encoded = value.encode();

            expect(encoded.type).toBe('Boolean');
            expect(encoded.value).toBe('true');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(true);
        });

        it('should encode false and be parseable', () => {
            const value = new BooleanValue(false);
            const encoded = value.encode();

            expect(encoded.type).toBe('Boolean');
            expect(encoded.value).toBe('false');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(false);
        });

        it('should encode a none Boolean through NoneValue', () => {
            // a none travels as an option type, so a bare Boolean can never carry the marker
            const encoded = new NoneValue('Boolean').encode();

            expect(encoded.type).toEqual({Option: 'Boolean'});
            expect(encoded.value).toBe(NONE_VALUE);

            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });
    });

    describe('Int4Value', () => {
        it('should encode positive number and be parseable', () => {
            const value = new Int4Value(42);
            const encoded = value.encode();

            expect(encoded.type).toBe('Int4');
            expect(encoded.value).toBe('42');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(42);
        });

        it('should encode negative number and be parseable', () => {
            const value = new Int4Value(-123);
            const encoded = value.encode();

            expect(encoded.type).toBe('Int4');
            expect(encoded.value).toBe('-123');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(-123);
        });

        it('should encode zero and be parseable', () => {
            const value = new Int4Value(0);
            const encoded = value.encode();

            expect(encoded.type).toBe('Int4');
            expect(encoded.value).toBe('0');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(0);
        });

        it('should encode a none Int4 through NoneValue', () => {
            // a none travels as an option type, so a bare Int4 can never carry the marker
            const encoded = new NoneValue('Int4').encode();

            expect(encoded.type).toEqual({Option: 'Int4'});
            expect(encoded.value).toBe(NONE_VALUE);

            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });
    });

    describe('Float4Value', () => {
        it('should encode positive float and be parseable', () => {
            const value = new Float4Value(3.14);
            const encoded = value.encode();

            expect(encoded.type).toBe('Float4');
            // Float4 has precision limitations, so check the encoded value is parseable
            expect(parseFloat(encoded.value)).toBeCloseTo(3.14);

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeCloseTo(3.14);
        });

        it('should encode negative float and be parseable', () => {
            const value = new Float4Value(-2.5);
            const encoded = value.encode();

            expect(encoded.type).toBe('Float4');
            expect(encoded.value).toBe('-2.5');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeCloseTo(-2.5);
        });
    });

    describe('Utf8Value', () => {
        it('should encode string and be parseable', () => {
            const value = new Utf8Value('hello world');
            const encoded = value.encode();

            expect(encoded.type).toBe('Utf8');
            expect(encoded.value).toBe('hello world');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe('hello world');
        });

        it('should encode empty string and be parseable', () => {
            const value = new Utf8Value('');
            const encoded = value.encode();

            expect(encoded.type).toBe('Utf8');
            expect(encoded.value).toBe('');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe('');
        });

        it('should encode unicode string and be parseable', () => {
            const value = new Utf8Value('🚀 Hello 世界');
            const encoded = value.encode();

            expect(encoded.type).toBe('Utf8');
            expect(encoded.value).toBe('🚀 Hello 世界');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe('🚀 Hello 世界');
        });
    });

    describe('DateValue', () => {
        it('should encode date and be parseable', () => {
            const value = new DateValue('2024-03-15');
            const encoded = value.encode();

            expect(encoded.type).toBe('Date');
            expect(encoded.value).toBe('2024-03-15');

            const decoded = decode(encoded);
            expect((decoded as DateValue).toString()).toBe('2024-03-15');
        });

        it('should encode leap year date and be parseable', () => {
            const value = new DateValue('2024-02-29');
            const encoded = value.encode();

            expect(encoded.type).toBe('Date');
            expect(encoded.value).toBe('2024-02-29');

            const decoded = decode(encoded);
            expect((decoded as DateValue).toString()).toBe('2024-02-29');
        });
    });

    describe('DurationValue', () => {
        it('should encode 1 hour duration and be parseable', () => {
            const value = DurationValue.fromHours(1);
            const encoded = value.encode();

            expect(encoded.type).toBe('Duration');
            expect(encoded.value).toBe('PT1H');

            const decoded = decode(encoded);
            expect((decoded as DurationValue).toIsoString()).toBe('PT1H');
        });

        it('should encode 5 days duration and be parseable', () => {
            const value = DurationValue.fromDays(5);
            const encoded = value.encode();

            expect(encoded.type).toBe('Duration');
            expect(encoded.value).toBe('P5D');

            const decoded = decode(encoded);
            expect((decoded as DurationValue).toIsoString()).toBe('P5D');
        });

        it('should encode zero duration and be parseable', () => {
            const value = DurationValue.zero();
            const encoded = value.encode();

            expect(encoded.type).toBe('Duration');
            expect(encoded.value).toBe('PT0S');

            const decoded = decode(encoded);
            expect((decoded as DurationValue).toIsoString()).toBe('PT0S');
        });
    });

    describe('Uuid4Value', () => {
        it('should encode UUID and be parseable', () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const value = new Uuid4Value(uuid);
            const encoded = value.encode();

            expect(encoded.type).toBe('Uuid4');
            expect(encoded.value).toBe(uuid);

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(uuid);
        });

        it('should encode nil UUID and be parseable', () => {
            const value = Uuid4Value.nil();
            const encoded = value.encode();

            expect(encoded.type).toBe('Uuid4');
            expect(encoded.value).toBe('00000000-0000-0000-0000-000000000000');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe('00000000-0000-0000-0000-000000000000');
        });
    });

    describe('NoneValue', () => {
        it('should encode none value as a typed none of its inner type', () => {
            const value = new NoneValue('Int4');
            const encoded = value.encode();

            expect(encoded.type).toEqual({Option: 'Int4'});
            expect(encoded.value).toBe(NONE_VALUE);

            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect((decoded as NoneValue).innerType).toBe('Int4');
            expect(decoded.valueOf()).toBeUndefined();
        });
    });

    describe('All integer types', () => {
        const intTypes = [
            {name: 'Int1Value', constructor: Int1Value, testValue: 42, expectBigInt: false},
            {name: 'Int2Value', constructor: Int2Value, testValue: 1000, expectBigInt: false},
            {name: 'Int8Value', constructor: Int8Value, testValue: 123456789, expectBigInt: true},
            {name: 'Int16Value', constructor: Int16Value, testValue: 12345, expectBigInt: true},
            {name: 'Uint1Value', constructor: Uint1Value, testValue: 42, expectBigInt: false},
            {name: 'Uint2Value', constructor: Uint2Value, testValue: 1000, expectBigInt: false},
            {name: 'Uint4Value', constructor: Uint4Value, testValue: 123456789, expectBigInt: false},
            {name: 'Uint8Value', constructor: Uint8Value, testValue: 123456789, expectBigInt: true},
            {name: 'Uint16Value', constructor: Uint16Value, testValue: 12345, expectBigInt: true}
        ] as const;

        intTypes.forEach(({name, constructor, testValue, expectBigInt}) => {
            it(`should encode ${name} and be parseable`, () => {
                const value = new constructor(testValue);
                const encoded = value.encode();

                expect(encoded.value).toBe(testValue.toString());

                const decoded = decode(encoded);
                if (expectBigInt) {
                    expect(decoded.valueOf()).toBe(BigInt(testValue));
                } else {
                    expect(decoded.valueOf()).toBe(testValue);
                }
            });
        });
    });

    describe('All float types', () => {
        it('should encode Float8Value and be parseable', () => {
            const value = new Float8Value(3.141592653589793);
            const encoded = value.encode();

            expect(encoded.type).toBe('Float8');
            expect(encoded.value).toBe('3.141592653589793');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeCloseTo(3.141592653589793);
        });
    });

    describe('None value encoding for all types', () => {
        it('should encode a none Blob through NoneValue', () => {
            // a none travels as an option type, so a bare Blob can never carry the marker
            const encoded = new NoneValue('Blob').encode();
            expect(encoded.type).toEqual({Option: 'Blob'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Date through NoneValue', () => {
            // a none travels as an option type, so a bare Date can never carry the marker
            const encoded = new NoneValue('Date').encode();
            expect(encoded.type).toEqual({Option: 'Date'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none DateTime through NoneValue', () => {
            // a none travels as an option type, so a bare DateTime can never carry the marker
            const encoded = new NoneValue('DateTime').encode();
            expect(encoded.type).toEqual({Option: 'DateTime'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Float4 through NoneValue', () => {
            // a none travels as an option type, so a bare Float4 can never carry the marker
            const encoded = new NoneValue('Float4').encode();
            expect(encoded.type).toEqual({Option: 'Float4'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Float8 through NoneValue', () => {
            // a none travels as an option type, so a bare Float8 can never carry the marker
            const encoded = new NoneValue('Float8').encode();
            expect(encoded.type).toEqual({Option: 'Float8'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Duration through NoneValue', () => {
            // a none travels as an option type, so a bare Duration can never carry the marker
            const encoded = new NoneValue('Duration').encode();
            expect(encoded.type).toEqual({Option: 'Duration'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Time through NoneValue', () => {
            // a none travels as an option type, so a bare Time can never carry the marker
            const encoded = new NoneValue('Time').encode();
            expect(encoded.type).toEqual({Option: 'Time'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Utf8 through NoneValue', () => {
            // a none travels as an option type, so a bare Utf8 can never carry the marker
            const encoded = new NoneValue('Utf8').encode();
            expect(encoded.type).toEqual({Option: 'Utf8'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Uuid4 through NoneValue', () => {
            // a none travels as an option type, so a bare Uuid4 can never carry the marker
            const encoded = new NoneValue('Uuid4').encode();
            expect(encoded.type).toEqual({Option: 'Uuid4'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Uuid7 through NoneValue', () => {
            // a none travels as an option type, so a bare Uuid7 can never carry the marker
            const encoded = new NoneValue('Uuid7').encode();
            expect(encoded.type).toEqual({Option: 'Uuid7'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none Decimal through NoneValue', () => {
            // a none travels as an option type, so a bare Decimal can never carry the marker
            const encoded = new NoneValue('Decimal').encode();
            expect(encoded.type).toEqual({Option: 'Decimal'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode a none IdentityId through NoneValue', () => {
            // a none travels as an option type, so a bare IdentityId can never carry the marker
            const encoded = new NoneValue('IdentityId').encode();
            expect(encoded.type).toEqual({Option: 'IdentityId'});
            expect(encoded.value).toBe(NONE_VALUE);
            const decoded = decode(encoded);
            expect(decoded).toBeInstanceOf(NoneValue);
            expect(decoded.valueOf()).toBeUndefined();
        });

        const allIntTypes = [
            'Int1', 'Int2', 'Int4', 'Int8', 'Int16',
            'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16'
        ] as const;

        allIntTypes.forEach(name => {
            it(`should encode a none ${name} through NoneValue`, () => {
                // a none travels as an option type, so a bare integer type can never carry the marker
                const encoded = new NoneValue(name).encode();
                expect(encoded.type).toEqual({Option: name});
                expect(encoded.value).toBe(NONE_VALUE);
                const decoded = decode(encoded);
                expect(decoded).toBeInstanceOf(NoneValue);
                expect(decoded.valueOf()).toBeUndefined();
            });
        });
    });
});
