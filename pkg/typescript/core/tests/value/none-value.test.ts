// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
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

    it('should reject NONE_VALUE for Blob', () => {
        // a non-option Blob must always be defined, so the none marker is not a Blob value
        expect(() => BlobValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Boolean', () => {
        // a non-option Boolean must always be defined, so the none marker is not a Boolean value
        expect(() => BooleanValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Utf8', () => {
        // a non-option Utf8 must always be defined, so the none marker is not a Utf8 value
        expect(() => Utf8Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Float4', () => {
        // a non-option Float4 must always be defined, so the none marker is not a Float4 value
        expect(() => Float4Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Float8', () => {
        // a non-option Float8 must always be defined, so the none marker is not a Float8 value
        expect(() => Float8Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Int1', () => {
        // a non-option Int1 must always be defined, so the none marker is not a Int1 value
        expect(() => Int1Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Int2', () => {
        // a non-option Int2 must always be defined, so the none marker is not a Int2 value
        expect(() => Int2Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Int4', () => {
        // a non-option Int4 must always be defined, so the none marker is not a Int4 value
        expect(() => Int4Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Int8', () => {
        // a non-option Int8 must always be defined, so the none marker is not a Int8 value
        expect(() => Int8Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Int16', () => {
        // a non-option Int16 must always be defined, so the none marker is not a Int16 value
        expect(() => Int16Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uint1', () => {
        // a non-option Uint1 must always be defined, so the none marker is not a Uint1 value
        expect(() => Uint1Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uint2', () => {
        // a non-option Uint2 must always be defined, so the none marker is not a Uint2 value
        expect(() => Uint2Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uint4', () => {
        // a non-option Uint4 must always be defined, so the none marker is not a Uint4 value
        expect(() => Uint4Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uint8', () => {
        // a non-option Uint8 must always be defined, so the none marker is not a Uint8 value
        expect(() => Uint8Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uint16', () => {
        // a non-option Uint16 must always be defined, so the none marker is not a Uint16 value
        expect(() => Uint16Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Date', () => {
        // a non-option Date must always be defined, so the none marker is not a Date value
        expect(() => DateValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Time', () => {
        // a non-option Time must always be defined, so the none marker is not a Time value
        expect(() => TimeValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for DateTime', () => {
        // a non-option DateTime must always be defined, so the none marker is not a DateTime value
        expect(() => DateTimeValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Duration', () => {
        // a non-option Duration must always be defined, so the none marker is not a Duration value
        expect(() => DurationValue.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uuid4', () => {
        // a non-option Uuid4 must always be defined, so the none marker is not a Uuid4 value
        expect(() => Uuid4Value.parse(noneString)).toThrow();
    });

    it('should reject NONE_VALUE for Uuid7', () => {
        // a non-option Uuid7 must always be defined, so the none marker is not a Uuid7 value
        expect(() => Uuid7Value.parse(noneString)).toThrow();
    });

    it('should parse NONE_VALUE for NoneValue', () => {
        const noneValue = NoneValue.parse(noneString);
        expect(noneValue.value).toBeUndefined();
        expect(noneValue.type).toBe('None');
    });
});
