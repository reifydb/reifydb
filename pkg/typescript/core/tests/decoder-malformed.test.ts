// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { describe, expect, it } from 'vitest';
import { columnsToRows, decode } from '../src/decoder';
import { envelopesToFrames, Type } from '../src/value';
import { NONE_VALUE } from '../src/constant';

const NON_OPTION_TYPES: Type[] = [
    'Int1', 'Int2', 'Int4', 'Int8', 'Int16',
    'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16',
    'Float4', 'Float8', 'Decimal', 'Boolean',
    'Date', 'DateTime', 'Time', 'Duration',
    'Uuid4', 'Uuid7', 'IdentityId', 'Blob',
];

function decodeEnvelopes(envelopes: any) {
    return envelopesToFrames(envelopes).map(frame => columnsToRows(frame.columns));
}

describe('decode rejects malformed cells', () => {
    it.each(NON_OPTION_TYPES)('rejects the none marker in a non-Option %s column', (type) => {
        // a non-Option column cannot hold a none, so the marker must not become a placeholder value
        expect(() => decode({ type, value: NONE_VALUE })).toThrow();
    });

    it.each(NON_OPTION_TYPES)('rejects an empty cell in a non-Option %s column', (type) => {
        // the server never renders a value as empty text, so an empty cell must not read as none
        expect(() => decode({ type, value: '' })).toThrow();
    });

    it('rejects an empty cell in an Option(Int4) column', () => {
        // a none arrives as the marker, so an empty cell is malformed rather than a none placeholder
        expect(() => decode({ type: { Option: 'Int4' }, value: '' })).toThrow();
    });

    it('rejects a Decimal cell that is not a number', () => {
        // an unparsable Decimal must fail loud instead of being carried through as text
        expect(() => decode({ type: 'Decimal', value: 'abc' })).toThrow();
    });
});

describe('columnsToRows rejects ragged columns', () => {
    it('rejects a later column shorter than the first', () => {
        // a missing cell must not decode as none in a non-Option Utf8 column
        expect(() => columnsToRows([
            { name: 'a', type: 'Int4', payload: ['1', '2'] },
            { name: 'b', type: 'Utf8', payload: ['x'] },
        ])).toThrow();
    });

    it('rejects a first column shorter than a later one', () => {
        // the row count must not come from the first column alone, otherwise later rows are dropped
        expect(() => columnsToRows([
            { name: 'a', type: 'Int4', payload: ['1'] },
            { name: 'b', type: 'Int4', payload: ['1', '2'] },
        ])).toThrow();
    });
});

describe('envelopesToFrames rejects malformed envelopes', () => {
    it('rejects an envelope list that is not an array', () => {
        // a malformed envelope list must not read as zero frames
        expect(() => envelopesToFrames({ rows: [] })).toThrow();
    });

    it('rejects an envelope with rows but no types', () => {
        // without types every column is lost, so the rows must not silently vanish
        expect(() => decodeEnvelopes([{ rows: [{ a: '1' }] }])).toThrow();
    });

    it('rejects a row that lacks a typed column', () => {
        // the server writes every column into every row, so an absent key must not read as none
        const envelopes = [{ types: { a: { id: 'Option', underlying: { id: 'Int4' } } }, rows: [{}] }];
        expect(() => decodeEnvelopes(envelopes)).toThrow();
    });

    it('rejects a bare none cell in a non-Option Int4 column', () => {
        // the server writes a bare none cell only for a none, which a non-Option column cannot hold
        const envelopes = [{ types: { a: { id: 'Int4' } }, rows: [{ a: null }] }];
        expect(() => decodeEnvelopes(envelopes)).toThrow();
    });

    it('rejects an object in a Utf8 column', () => {
        // an object cell must not be stringified into the text "[object Object]"
        const envelopes = [{ types: { a: { id: 'Utf8' } }, rows: [{ a: { x: 1 } }] }];
        expect(() => decodeEnvelopes(envelopes)).toThrow();
    });

    it('rejects a JSON number in an Int8 column', () => {
        // JSON.parse already rounded a number cell, so only a text cell can be exact
        const envelopes = JSON.parse('[{"types":{"a":{"id":"Int8"}},"rows":[{"a":9007199254740993}]}]');
        expect(() => decodeEnvelopes(envelopes)).toThrow();
    });
});
