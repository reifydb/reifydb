/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {
    BlobValue,
    BoolValue,
    DateValue,
    DateTimeValue,
    Float4Value,
    Float8Value, Int16Value,
    Int1Value,
    Int2Value,
    Int4Value, Int8Value,
    IntervalValue,
    RowIdValue,
    TimeValue,
    Uint16Value, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    Utf8Value,
    Uuid4Value,
    Uuid7Value
} from "../../src/value";


describe('UNDEFINED_VALUE parsing', () => {
    const undefinedString = "⟪undefined⟫";

    it('should parse UNDEFINED_VALUE as undefined for Blob', () => {
        const blob = BlobValue.parse(undefinedString);
        expect(blob.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Bool', () => {
        const bool = BoolValue.parse(undefinedString);
        expect(bool.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Utf8', () => {
        const utf8 = Utf8Value.parse(undefinedString);
        expect(utf8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for RowId', () => {
        const rowId = RowIdValue.parse(undefinedString);
        expect(rowId.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Float4', () => {
        const float4 = Float4Value.parse(undefinedString);
        expect(float4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Float8', () => {
        const float8 = Float8Value.parse(undefinedString);
        expect(float8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int1', () => {
        const int1 = Int1Value.parse(undefinedString);
        expect(int1.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int2', () => {
        const int2 = Int2Value.parse(undefinedString);
        expect(int2.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int4', () => {
        const int4 = Int4Value.parse(undefinedString);
        expect(int4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int8', () => {
        const int8 = Int8Value.parse(undefinedString);
        expect(int8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int16', () => {
        const int16 = Int16Value.parse(undefinedString);
        expect(int16.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint1', () => {
        const uint1 = Uint1Value.parse(undefinedString);
        expect(uint1.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint2', () => {
        const uint2 = Uint2Value.parse(undefinedString);
        expect(uint2.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint4', () => {
        const uint4 = Uint4Value.parse(undefinedString);
        expect(uint4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint8', () => {
        const uint8 = Uint8Value.parse(undefinedString);
        expect(uint8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint16', () => {
        const uint16 = Uint16Value.parse(undefinedString);
        expect(uint16.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Date', () => {
        const date = DateValue.parse(undefinedString);
        expect(date.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Time', () => {
        const time = TimeValue.parse(undefinedString);
        expect(time.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for DateTime', () => {
        const datetime = DateTimeValue.parse(undefinedString);
        expect(datetime.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Interval', () => {
        const interval = IntervalValue.parse(undefinedString);
        expect(interval.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uuid4', () => {
        const uuid4 = Uuid4Value.parse(undefinedString);
        expect(uuid4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uuid7', () => {
        const uuid7 = Uuid7Value.parse(undefinedString);
        expect(uuid7.value).toBeUndefined();
    });
});