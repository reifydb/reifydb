// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {removeRows, upsertRows} from '../src/entry';
import type {Entry} from '../src';

interface Item {
    id: number;
    name: string;
}

const empty: Entry<Item> = {status: 'loading', rows: new Map(), data: [], error: undefined};

describe('row merge', () => {
    it('insert adds rows keyed by rownum in insertion order', () => {
        const entry = upsertRows(empty, [
            {'#rownum': 7, id: 1, name: 'a'},
            {'#rownum': 3, id: 2, name: 'b'},
        ]);
        expect(Array.from(entry.rows.keys())).toEqual([7, 3]);
        expect(entry.data).toEqual([{id: 1, name: 'a'}, {id: 2, name: 'b'}]);
    });

    it('exposed rows do not carry the rownum field', () => {
        const entry = upsertRows(empty, [{'#rownum': 7, id: 1, name: 'a'}]);
        expect(Object.keys(entry.rows.get(7)!)).toEqual(['id', 'name']);
    });

    it('update replaces the row with that rownum in place', () => {
        const before = upsertRows(empty, [
            {'#rownum': 7, id: 1, name: 'a'},
            {'#rownum': 3, id: 2, name: 'b'},
        ]);
        const after = upsertRows(before, [{'#rownum': 7, id: 1, name: 'a2'}]);
        expect(after.data).toEqual([{id: 1, name: 'a2'}, {id: 2, name: 'b'}]);
    });

    it('remove drops the row with that rownum', () => {
        const before = upsertRows(empty, [
            {'#rownum': 7, id: 1, name: 'a'},
            {'#rownum': 3, id: 2, name: 'b'},
        ]);
        const after = removeRows(before, [{'#rownum': 7, id: 1, name: 'a'}]);
        expect(after.data).toEqual([{id: 2, name: 'b'}]);
        expect(after.rows.has(7)).toBe(false);
    });

    it('remove of an unknown rownum is a no-op that returns the same entry', () => {
        const before = upsertRows(empty, [{'#rownum': 7, id: 1, name: 'a'}]);
        expect(removeRows(before, [{'#rownum': 99, id: 9, name: 'z'}])).toBe(before);
    });

    it('insert of an already present rownum replaces it, keeping re-hydration idempotent', () => {
        const before = upsertRows(empty, [{'#rownum': 7, id: 1, name: 'a'}]);
        const after = upsertRows(before, [{'#rownum': 7, id: 1, name: 'a'}]);
        expect(after.rows.size).toBe(1);
        expect(after.data).toEqual([{id: 1, name: 'a'}]);
    });

    it('data keeps its reference when nothing changed and gets a new one when anything did', () => {
        const before = upsertRows(empty, [{'#rownum': 7, id: 1, name: 'a'}]);
        expect(removeRows(before, [{'#rownum': 99, id: 9, name: 'z'}]).data).toBe(before.data);
        expect(upsertRows(before, [{'#rownum': 7, id: 1, name: 'a'}]).data).not.toBe(before.data);
        expect(removeRows(before, [{'#rownum': 7, id: 1, name: 'a'}]).data).not.toBe(before.data);
    });
});
