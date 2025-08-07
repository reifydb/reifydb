/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Int1} from '../../src/value/int1';
import {Int2} from '../../src/value/int2';
import {Int4} from '../../src/value/int4';
import {Int8} from '../../src/value/int8';
import {Int16} from '../../src/value/int16';
import {Uint1} from '../../src/value/uint1';
import {Uint2} from '../../src/value/uint2';
import {Uint4} from '../../src/value/uint4';
import {Uint8} from '../../src/value/uint8';
import {Uint16} from '../../src/value/uint16';
import {Float4} from '../../src/value/float4';
import {Float8} from '../../src/value/float8';


describe('UNDEFINED_VALUE parsing', () => {
    const undefinedString = "⟪undefined⟫";

    it('should parse UNDEFINED_VALUE as undefined for Float4', () => {
        const float4 = Float4.parse(undefinedString);
        expect(float4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Float8', () => {
        const float8 = Float8.parse(undefinedString);
        expect(float8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int1', () => {
        const int1 = Int1.parse(undefinedString);
        expect(int1.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int2', () => {
        const int2 = Int2.parse(undefinedString);
        expect(int2.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int4', () => {
        const int4 = Int4.parse(undefinedString);
        expect(int4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int8', () => {
        const int8 = Int8.parse(undefinedString);
        expect(int8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Int16', () => {
        const int16 = Int16.parse(undefinedString);
        expect(int16.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint1', () => {
        const uint1 = Uint1.parse(undefinedString);
        expect(uint1.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint2', () => {
        const uint2 = Uint2.parse(undefinedString);
        expect(uint2.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint4', () => {
        const uint4 = Uint4.parse(undefinedString);
        expect(uint4.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint8', () => {
        const uint8 = Uint8.parse(undefinedString);
        expect(uint8.value).toBeUndefined();
    });

    it('should parse UNDEFINED_VALUE as undefined for Uint16', () => {
        const uint16 = Uint16.parse(undefinedString);
        expect(uint16.value).toBeUndefined();
    });
});