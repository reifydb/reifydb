// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {
    BlobValue,
    BooleanValue,
    DateValue,
    DateTimeValue,
    Float4Value,
    Float8Value, Int16Value,
    Int1Value,
    Int2Value,
    Int4Value, Int8Value,
    DurationValue,
    TimeValue,
    Uint16Value, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    NoneValue,
    Utf8Value,
    Uuid4Value,
    Uuid7Value
} from "../../src/value";


describe('NONE_VALUE parsing', () => {
    const noneString = "⟪none⟫";

    it('should parse NONE_VALUE as undefined for Blob', () => {
        const blob = BlobValue.parse(noneString);
        expect(blob.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Boolean', () => {
        const bool = BooleanValue.parse(noneString);
        expect(bool.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Utf8', () => {
        const utf8 = Utf8Value.parse(noneString);
        expect(utf8.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Float4', () => {
        const float4 = Float4Value.parse(noneString);
        expect(float4.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Float8', () => {
        const float8 = Float8Value.parse(noneString);
        expect(float8.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Int1', () => {
        const int1 = Int1Value.parse(noneString);
        expect(int1.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Int2', () => {
        const int2 = Int2Value.parse(noneString);
        expect(int2.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Int4', () => {
        const int4 = Int4Value.parse(noneString);
        expect(int4.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Int8', () => {
        const int8 = Int8Value.parse(noneString);
        expect(int8.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Int16', () => {
        const int16 = Int16Value.parse(noneString);
        expect(int16.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uint1', () => {
        const uint1 = Uint1Value.parse(noneString);
        expect(uint1.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uint2', () => {
        const uint2 = Uint2Value.parse(noneString);
        expect(uint2.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uint4', () => {
        const uint4 = Uint4Value.parse(noneString);
        expect(uint4.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uint8', () => {
        const uint8 = Uint8Value.parse(noneString);
        expect(uint8.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uint16', () => {
        const uint16 = Uint16Value.parse(noneString);
        expect(uint16.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Date', () => {
        const date = DateValue.parse(noneString);
        expect(date.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Time', () => {
        const time = TimeValue.parse(noneString);
        expect(time.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for DateTime', () => {
        const datetime = DateTimeValue.parse(noneString);
        expect(datetime.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Duration', () => {
        const duration = DurationValue.parse(noneString);
        expect(duration.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uuid4', () => {
        const uuid4 = Uuid4Value.parse(noneString);
        expect(uuid4.value).toBeUndefined();
    });

    it('should parse NONE_VALUE as undefined for Uuid7', () => {
        const uuid7 = Uuid7Value.parse(noneString);
        expect(uuid7.value).toBeUndefined();
    });

    it('should parse NONE_VALUE for NoneValue', () => {
        const noneValue = NoneValue.parse(noneString);
        expect(noneValue.value).toBeUndefined();
        expect(noneValue.type).toBe('None');
    });
});
