// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { describe, expect, it } from 'vitest';
import { decode } from '../src/decoder';
import { encodeValue } from '../src/encoder';
import { EnumVariant, Utf8Value } from '../src/value';

describe('EnumVariant', () => {
    it('decodes an enum cell into the variant of its column enum', () => {
        // A cell names its enum, so a value copied out of its column still says which enum it belongs to.
        const value = decode({type: {Enum: 's::status'}, value: 'doing'});
        expect(value).toBeInstanceOf(EnumVariant);
        expect((value as EnumVariant).enum).toBe('s::status');
        expect((value as EnumVariant).variant).toBe('doing');
    });

    it('encodes back to the enum type with the variant as the cell', () => {
        // A value read back must be sendable as a param unchanged, typed as its enum.
        expect(encodeValue(new EnumVariant('s::status', 'doing'))).toEqual({type: {Enum: 's::status'}, value: 'doing'});
    });

    it('is equal only to the same variant of the same enum', () => {
        // Equality covers both parts; the same name in another enum or plain text is a different value.
        const doing = new EnumVariant('s::status', 'doing');
        expect(doing.equals(new EnumVariant('s::status', 'doing'))).toBe(true);
        expect(doing.equals(new EnumVariant('s::other', 'doing'))).toBe(false);
        expect(doing.equals(new Utf8Value('doing'))).toBe(false);
    });
});
