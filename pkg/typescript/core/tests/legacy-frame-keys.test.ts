// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { describe, expect, it } from 'vitest';
import { columnsToRows } from '../src/decoder';
import { envelopesToFrames, framesFromWire } from '../src/value';

const COLUMNS = [{ name: 'id', type: { id: 'Int4' }, payload: ['1'] }];

describe('framesFromWire rejects the legacy frame keys', () => {
    it.each(['row_numbers', 'created_at', 'updated_at', 'time'])('rejects a non-empty %s', (key) => {
        // Rust rejects these keys, so a client that reads them would decode a frame the server side refuses
        expect(() => framesFromWire([{ columns: COLUMNS, [key]: [1] }]))
            .toThrow(`frame key ${key} is not supported, system columns travel as # columns`);
    });

    it.each(['row_numbers', 'created_at', 'updated_at', 'time'])('accepts an empty %s', (key) => {
        // an empty array carries nothing, so it must not fail a frame that Rust accepts
        expect(() => framesFromWire([{ columns: COLUMNS, [key]: [] }])).not.toThrow();
    });
});

describe('#rownum keeps travelling as a column', () => {
    it('envelopesToFrames still turns a #rownum cell into the row number of the row', () => {
        // #rownum is the one system column that is always sent, so rejecting legacy keys must not break it
        const [frame] = envelopesToFrames([{
            types: { id: { id: 'Int4' } },
            rows: [{ id: '7', '#rownum': 1 }, { id: '8', '#rownum': 2 }],
        }]);
        const rows = columnsToRows(frame.columns, frame.row_numbers);
        expect(rows.map((row: any) => row['#rownum'])).toEqual([1, 2]);
    });
});
