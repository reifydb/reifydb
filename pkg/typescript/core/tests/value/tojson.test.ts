// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import {
    BlobValue,
    BooleanValue,
    DateValue,
    DateTimeValue,
    DecimalValue,
    DurationValue,
    Float4Value,
    Float8Value,
    IdentityIdValue,
    Int1Value,
    Int2Value,
    Int4Value,
    Int8Value,
    Int16Value,
    NoneValue,
    TimeValue,
    Uint1Value,
    Uint2Value,
    Uint4Value,
    Uint8Value,
    Uint16Value,
    Utf8Value,
    Uuid4Value,
    Uuid7Value,
} from '../../src';

describe('toJSON', () => {
    describe('numeric types serialize as strings', () => {
        it('Int1Value', () => {
            expect(new Int1Value(42).toJSON()).toBe('42');
            expect(new Int1Value(-128).toJSON()).toBe('-128');
            expect(new Int1Value(0).toJSON()).toBe('0');
        });

        it('Int2Value', () => {
            expect(new Int2Value(1234).toJSON()).toBe('1234');
            expect(new Int2Value(-32768).toJSON()).toBe('-32768');
        });

        it('Int4Value', () => {
            expect(new Int4Value(2147483647).toJSON()).toBe('2147483647');
            expect(new Int4Value(-2147483648).toJSON()).toBe('-2147483648');
        });

        it('Int8Value', () => {
            expect(new Int8Value(BigInt('9223372036854775807')).toJSON()).toBe('9223372036854775807');
            expect(new Int8Value(BigInt('-9223372036854775808')).toJSON()).toBe('-9223372036854775808');
        });

        it('Int16Value', () => {
            expect(new Int16Value(BigInt('170141183460469231731687303715884105727')).toJSON()).toBe(
                '170141183460469231731687303715884105727',
            );
        });

        it('Uint1Value', () => {
            expect(new Uint1Value(255).toJSON()).toBe('255');
            expect(new Uint1Value(0).toJSON()).toBe('0');
        });

        it('Uint2Value', () => {
            expect(new Uint2Value(65535).toJSON()).toBe('65535');
        });

        it('Uint4Value', () => {
            expect(new Uint4Value(4294967295).toJSON()).toBe('4294967295');
            expect(new Uint4Value(128).toJSON()).toBe('128');
        });

        it('Uint8Value', () => {
            expect(new Uint8Value(BigInt('18446744073709551615')).toJSON()).toBe('18446744073709551615');
            expect(new Uint8Value(BigInt(2200180)).toJSON()).toBe('2200180');
            expect(new Uint8Value(BigInt(0)).toJSON()).toBe('0');
        });

        it('Uint16Value', () => {
            expect(new Uint16Value(BigInt('340282366920938463463374607431768211455')).toJSON()).toBe(
                '340282366920938463463374607431768211455',
            );
        });

        it('Float4Value', () => {
            expect(new Float4Value(1.5).toJSON()).toBe('1.5');
            expect(new Float4Value(0).toJSON()).toBe('0');
        });

        it('Float8Value', () => {
            expect(new Float8Value(34.244235695986326).toJSON()).toBe('34.244235695986326');
            expect(new Float8Value(0).toJSON()).toBe('0');
            expect(new Float8Value(-0).toJSON()).toBe('0');
        });

        it('DecimalValue', () => {
            expect(new DecimalValue('123.456789012345678901234567890').toJSON()).toBe(
                '123.456789012345678901234567890',
            );
        });
    });

    describe('non-numeric types serialize naturally', () => {
        it('BooleanValue returns boolean', () => {
            expect(new BooleanValue(true).toJSON()).toBe(true);
            expect(new BooleanValue(false).toJSON()).toBe(false);
        });

        it('Utf8Value returns the string', () => {
            expect(new Utf8Value('hello').toJSON()).toBe('hello');
            expect(new Utf8Value('').toJSON()).toBe('');
        });

        it('Uuid4Value returns the uuid string', () => {
            const id = '00000000-0000-4000-8000-000000000001';
            expect(new Uuid4Value(id).toJSON()).toBe(id);
        });

        it('Uuid7Value returns the uuid string', () => {
            const id = '01890000-0000-7000-8000-000000000001';
            expect(new Uuid7Value(id).toJSON()).toBe(id);
        });

        it('IdentityIdValue returns the id string', () => {
            const id = '01890000-0000-7000-8000-000000000002';
            expect(new IdentityIdValue(id).toJSON()).toBe(id);
        });

        it('BlobValue returns hex string with 0x prefix', () => {
            expect(new BlobValue(new Uint8Array([0xab, 0xcd])).toJSON()).toBe('0xabcd');
        });

        it('DateValue returns ISO date string', () => {
            const d = DateValue.fromYMD(2026, 5, 1);
            expect(d.toJSON()).toBe(d.toString());
        });

        it('DateTimeValue returns ISO datetime string', () => {
            const dt = DateTimeValue.fromYMDHMS(2026, 5, 1, 12, 34, 56);
            expect(dt.toJSON()).toBe(dt.toString());
        });

        it('TimeValue returns time string', () => {
            const t = TimeValue.fromHMS(12, 34, 56);
            expect(t.toJSON()).toBe(t.toString());
        });

        it('DurationValue returns ISO duration string', () => {
            expect(DurationValue.fromSeconds(30).toJSON()).toBe('PT30S');
            expect(DurationValue.zero().toJSON()).toBe('PT0S');
        });
    });

    describe('unset values serialize as null', () => {
        it('every numeric class returns null for undefined', () => {
            expect(new Int1Value(undefined).toJSON()).toBeNull();
            expect(new Int2Value(undefined).toJSON()).toBeNull();
            expect(new Int4Value(undefined).toJSON()).toBeNull();
            expect(new Int8Value(undefined).toJSON()).toBeNull();
            expect(new Int16Value(undefined).toJSON()).toBeNull();
            expect(new Uint1Value(undefined).toJSON()).toBeNull();
            expect(new Uint2Value(undefined).toJSON()).toBeNull();
            expect(new Uint4Value(undefined).toJSON()).toBeNull();
            expect(new Uint8Value(undefined).toJSON()).toBeNull();
            expect(new Uint16Value(undefined).toJSON()).toBeNull();
            expect(new Float4Value(undefined).toJSON()).toBeNull();
            expect(new Float8Value(undefined).toJSON()).toBeNull();
            expect(new DecimalValue(undefined).toJSON()).toBeNull();
        });

        it('every non-numeric class returns null for undefined', () => {
            expect(new BooleanValue(undefined).toJSON()).toBeNull();
            expect(new Utf8Value(undefined).toJSON()).toBeNull();
            expect(new Uuid4Value(undefined).toJSON()).toBeNull();
            expect(new Uuid7Value(undefined).toJSON()).toBeNull();
            expect(new IdentityIdValue(undefined).toJSON()).toBeNull();
            expect(new BlobValue(undefined).toJSON()).toBeNull();
            expect(new DateValue(undefined).toJSON()).toBeNull();
            expect(new DateTimeValue(undefined).toJSON()).toBeNull();
            expect(new TimeValue(undefined).toJSON()).toBeNull();
            expect(new DurationValue(undefined).toJSON()).toBeNull();
        });

        it('NoneValue returns null', () => {
            expect(new NoneValue().toJSON()).toBeNull();
        });
    });

    describe('JSON.stringify integration', () => {
        it('Uint8Value (bigint-backed) does not throw and emits a quoted string', () => {
            expect(JSON.stringify({ x: new Uint8Value(BigInt('18446744073709551615')) })).toBe(
                '{"x":"18446744073709551615"}',
            );
        });

        it('Float8Value emits a quoted string, not a JSON number', () => {
            expect(JSON.stringify({ x: new Float8Value(34.244235695986326) })).toBe(
                '{"x":"34.244235695986326"}',
            );
        });

        it('Uint4Value (number-backed) emits a quoted string for consistency', () => {
            expect(JSON.stringify({ x: new Uint4Value(128) })).toBe('{"x":"128"}');
        });

        it('mixed-type row produces uniform string numerics, native booleans, null for unset', () => {
            const row = {
                flag: new BooleanValue(true),
                count: new Uint4Value(128),
                big: new Uint8Value(BigInt('18446744073709551615')),
                ratio: new Float8Value(34.244235695986326),
                zero: new Float8Value(0),
                nothing: new Uint8Value(undefined),
                label: new Utf8Value('hello'),
            };
            expect(JSON.stringify(row)).toBe(
                '{"flag":true,"count":"128","big":"18446744073709551615","ratio":"34.244235695986326","zero":"0","nothing":null,"label":"hello"}',
            );
        });

        it('arrays of rows roundtrip without bigint errors', () => {
            const rows = [
                { id: new Uint8Value(BigInt(1)), score: new Float8Value(1.1) },
                { id: new Uint8Value(BigInt(2)), score: new Float8Value(2.2) },
            ];
            expect(JSON.stringify(rows)).toBe(
                '[{"id":"1","score":"1.1"},{"id":"2","score":"2.2"}]',
            );
        });
    });
});
