// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from "vitest";
import {widen, widens} from "../../src/shape/widen";
import {Float4Value, Float8Value, Int1Value, Int2Value, Int4Value, Int8Value, NoneValue, Uint1Value, Uint2Value, Utf8Value} from "../../src/value";

// A value shape names the type the caller wants to hold. The server picks the narrowest type a literal
// fits, so a caller asking for an Int4Value must still get one for a column the server sent as Int1.
// These tests pin the boundary in both directions: what widening accepts, and what it must refuse. The
// refusals matter more than the acceptances, because a widening that is too generous loses range or
// changes what a value means without anyone noticing.

describe('widens', () => {
    it('accepts a narrower signed source', () => {
        expect(widens('Int4', 'Int1')).toBe(true);
        expect(widens('Int16', 'Int8')).toBe(true);
    });

    it('accepts the same type, so an exact shape is unaffected', () => {
        expect(widens('Int4', 'Int4')).toBe(true);
        expect(widens('Utf8', 'Utf8')).toBe(true);
    });

    it('refuses a wider source, since the target would drop range', () => {
        expect(widens('Int1', 'Int4')).toBe(false);
        expect(widens('Float4', 'Float8')).toBe(false);
        expect(widens('Uint2', 'Uint8')).toBe(false);
    });

    it('refuses to cross between integer and float, since neither holds the other in full', () => {
        expect(widens('Float8', 'Int4')).toBe(false);
        expect(widens('Int8', 'Float4')).toBe(false);
    });

    it('takes a signed target for an unsigned source only when strictly wider, since Uint1 reaches 255', () => {
        expect(widens('Int1', 'Uint1')).toBe(false);
        expect(widens('Int2', 'Uint1')).toBe(true);
        expect(widens('Int8', 'Uint4')).toBe(true);
        expect(widens('Int8', 'Uint8')).toBe(false);
    });

    it('refuses an unsigned target for a signed source, since a negative has nowhere to go', () => {
        expect(widens('Uint8', 'Int1')).toBe(false);
        expect(widens('Uint16', 'Int4')).toBe(false);
    });

    it('refuses to cross families entirely', () => {
        expect(widens('Utf8', 'Int4')).toBe(false);
        expect(widens('Int4', 'Utf8')).toBe(false);
        expect(widens('Boolean', 'Int1')).toBe(false);
    });

    it('refuses an option on either side, since an option layer is not a width', () => {
        expect(widens('Int4', {Option: 'Int1'})).toBe(false);
    });
});

describe('widen', () => {
    it('re-reads a value as the wider type the shape asked for', () => {
        const widened = widen(new Int1Value(42), 'Int4');
        expect(widened).toBeInstanceOf(Int4Value);
        expect(widened.type).toBe('Int4');
        expect(widened.value).toBe(42);
    });

    it('crosses the number/bigint boundary, since Int8 and wider are bigints', () => {
        const widened = widen(new Int4Value(42), 'Int8');
        expect(widened).toBeInstanceOf(Int8Value);
        expect(widened.value).toBe(BigInt(42));
    });

    it('widens an unsigned source into a strictly wider signed target', () => {
        const widened = widen(new Uint1Value(255), 'Int2');
        expect(widened).toBeInstanceOf(Int2Value);
        expect(widened.value).toBe(255);
    });

    it('widens a float without disturbing the value', () => {
        const widened = widen(new Float4Value(1.5), 'Float8');
        expect(widened).toBeInstanceOf(Float8Value);
        expect(widened.value).toBe(1.5);
    });

    it('hands back the same value when the type already matches', () => {
        const original = new Int4Value(7);
        expect(widen(original, 'Int4')).toBe(original);
    });

    it('hands back the value untouched when widening is not allowed', () => {
        const wide = new Int4Value(70000);
        expect(widen(wide, 'Int1')).toBe(wide);
        const text = new Utf8Value('7');
        expect(widen(text, 'Int4')).toBe(text);
        const unsigned = new Uint2Value(7);
        expect(widen(unsigned, 'Int1')).toBe(unsigned);
    });

    it('leaves a none alone, since it carries no value to re-read', () => {
        const none = new NoneValue('Int1');
        expect(widen(none, 'Int4')).toBe(none);
    });

    it('leaves a plain primitive alone, since only a Value carries a type to widen from', () => {
        expect(widen(42, 'Int4')).toBe(42);
        expect(widen('42', 'Int4')).toBe('42');
        expect(widen(undefined, 'Int4')).toBe(undefined);
    });
});
