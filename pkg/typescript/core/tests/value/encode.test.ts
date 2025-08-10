/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {
    BlobValue, BoolValue, DateValue, DateTimeValue, Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, IntervalValue,
    RowIdValue, TimeValue, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    Uint16Value, UndefinedValue, Utf8Value, Uuid4Value, Uuid7Value, decode
} from '../../src';
import {UNDEFINED_VALUE} from '../../src/constant';

describe('Value encode method', () => {
    describe('BoolValue', () => {
        it('should encode true and be parseable', () => {
            const value = new BoolValue(true);
            const encoded = value.encode();

            expect(encoded.type).toBe('Bool');
            expect(encoded.value).toBe('true');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(true);
        });

        it('should encode false and be parseable', () => {
            const value = new BoolValue(false);
            const encoded = value.encode();

            expect(encoded.type).toBe('Bool');
            expect(encoded.value).toBe('false');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(false);
        });

        it('should encode undefined as UNDEFINED_VALUE', () => {
            const value = new BoolValue(undefined);
            const encoded = value.encode();

            expect(encoded.type).toBe('Bool');
            expect(encoded.value).toBe(UNDEFINED_VALUE);

            const decoded = decode(encoded);
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

        it('should encode undefined as UNDEFINED_VALUE', () => {
            const value = new Int4Value(undefined);
            const encoded = value.encode();

            expect(encoded.type).toBe('Int4');
            expect(encoded.value).toBe(UNDEFINED_VALUE);

            const decoded = decode(encoded);
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

    describe('IntervalValue', () => {
        it('should encode 1 hour interval and be parseable', () => {
            const value = IntervalValue.fromHours(1);
            const encoded = value.encode();

            expect(encoded.type).toBe('Interval');
            expect(encoded.value).toBe('PT1H');

            const decoded = decode(encoded);
            expect((decoded as IntervalValue).toString()).toBe('PT1H');
        });

        it('should encode 5 days interval and be parseable', () => {
            const value = IntervalValue.fromDays(5);
            const encoded = value.encode();

            expect(encoded.type).toBe('Interval');
            expect(encoded.value).toBe('P5D');

            const decoded = decode(encoded);
            expect((decoded as IntervalValue).toString()).toBe('P5D');
        });

        it('should encode zero interval and be parseable', () => {
            const value = IntervalValue.zero();
            const encoded = value.encode();

            expect(encoded.type).toBe('Interval');
            expect(encoded.value).toBe('PT0S');

            const decoded = decode(encoded);
            expect((decoded as IntervalValue).toString()).toBe('PT0S');
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

    describe('UndefinedValue', () => {
        it('should encode undefined value as UNDEFINED_VALUE', () => {
            const value = new UndefinedValue();
            const encoded = value.encode();

            expect(encoded.type).toBe('Undefined');
            expect(encoded.value).toBe(UNDEFINED_VALUE);

            const decoded = decode(encoded);
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

    describe('RowIdValue', () => {
        it('should encode RowId and be parseable', () => {
            const value = new RowIdValue(42);
            const encoded = value.encode();

            expect(encoded.type).toBe('RowId');
            expect(encoded.value).toBe('42');

            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBe(BigInt(42));
        });
    });

    describe('Undefined value encoding for all types', () => {
        it('should encode undefined BlobValue', () => {
            const value = new BlobValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined DateValue', () => {
            const value = new DateValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined DateTimeValue', () => {
            const value = new DateTimeValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined Float4Value', () => {
            const value = new Float4Value(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined Float8Value', () => {
            const value = new Float8Value(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined IntervalValue', () => {
            const value = new IntervalValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined TimeValue', () => {
            const value = new TimeValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined Utf8Value', () => {
            const value = new Utf8Value(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined Uuid4Value', () => {
            const value = new Uuid4Value(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined Uuid7Value', () => {
            const value = new Uuid7Value(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        it('should encode undefined RowIdValue', () => {
            const value = new RowIdValue(undefined);
            const encoded = value.encode();
            expect(encoded.value).toBe(UNDEFINED_VALUE);
            const decoded = decode(encoded);
            expect(decoded.valueOf()).toBeUndefined();
        });

        const allIntTypes = [
            {name: 'Int1Value', constructor: Int1Value},
            {name: 'Int2Value', constructor: Int2Value},
            {name: 'Int8Value', constructor: Int8Value},
            {name: 'Int16Value', constructor: Int16Value},
            {name: 'Uint1Value', constructor: Uint1Value},
            {name: 'Uint2Value', constructor: Uint2Value},
            {name: 'Uint4Value', constructor: Uint4Value},
            {name: 'Uint8Value', constructor: Uint8Value},
            {name: 'Uint16Value', constructor: Uint16Value}
        ] as const;

        allIntTypes.forEach(({name, constructor}) => {
            it(`should encode undefined ${name}`, () => {
                const value = new constructor(undefined);
                const encoded = value.encode();
                expect(encoded.value).toBe(UNDEFINED_VALUE);
                const decoded = decode(encoded);
                expect(decoded.valueOf()).toBeUndefined();
            });
        });
    });
});