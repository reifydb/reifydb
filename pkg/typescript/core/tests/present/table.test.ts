// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {formatValue, inferColumns, planTable, valueRole} from '../../src/present';
import {Int4Value, NoneValue, Utf8Value} from '../../src/value';

describe('formatValue', () => {
    it('renders a missing value as none, never as undefined or null', () => {
        // reifydb has no null - the shell used to print the literal string "undefined" here
        expect(formatValue(null)).toBe('none');
        expect(formatValue(undefined)).toBe('none');
        expect(formatValue(new NoneValue())).toBe('none');
    });

    it('renders a bigint without throwing', () => {
        expect(formatValue(9007199254740993n)).toBe('9007199254740993');
    });
});

describe('valueRole', () => {
    it('reads the role from the value type, not the javascript type', () => {
        expect(valueRole(new Int4Value(4))).toBe('number');
        expect(valueRole(new Utf8Value('4'))).toBe('string');
    });

    it('treats a missing value as none', () => {
        expect(valueRole(new NoneValue())).toBe('none');
        expect(valueRole(undefined)).toBe('none');
    });
});

describe('inferColumns', () => {
    it('right-aligns numeric columns and left-aligns the rest', () => {
        const columns = inferColumns([{age: new Int4Value(34), name: new Utf8Value('alice')}]);
        expect(columns.map((c) => [c.name, c.align])).toEqual([
            ['age', 'right'],
            ['name', 'left'],
        ]);
    });

    it('skips leading none values when deciding a column type', () => {
        // one missing value at the top must not flip the whole column to left-aligned text
        const columns = inferColumns([
            {age: new NoneValue()},
            {age: new Int4Value(34)},
        ]);
        expect(columns[0].align).toBe('right');
        expect(columns[0].role).toBe('number');
    });

    it('keeps first-seen column order across ragged rows', () => {
        const columns = inferColumns([{a: 1}, {b: 2}, {a: 3, c: 4}]);
        expect(columns.map((c) => c.name)).toEqual(['a', 'b', 'c']);
    });
});

describe('planTable', () => {
    const rows = [
        {name: new Utf8Value('alice'), role: new Utf8Value('admin')},
        {name: new Utf8Value('bob'), role: new Utf8Value('user')},
    ];

    it('sizes each column to the widest of its header and values', () => {
        const plan = planTable(rows, inferColumns(rows));
        expect(plan.columns.map((c) => c.width)).toEqual([5, 5]);
    });

    it('reports the columns it had to drop instead of dropping them silently', () => {
        // a dropped column used to vanish with no trace, so a result looked complete when it was not
        const plan = planTable(rows, inferColumns(rows), {width: 12});
        expect(plan.columns.map((c) => c.column.name)).toEqual(['name']);
        expect(plan.dropped.map((c) => c.name)).toEqual(['role']);
    });

    it('keeps one squeezed column when even the first does not fit', () => {
        const plan = planTable(rows, inferColumns(rows), {width: 6});
        expect(plan.columns).toHaveLength(1);
        expect(plan.columns[0].width).toBeGreaterThan(0);
        expect(plan.truncated).toBe(true);
    });

    it('fits every column when the width allows it', () => {
        const plan = planTable(rows, inferColumns(rows), {width: 80});
        expect(plan.dropped).toHaveLength(0);
    });

    it('caps a runaway column instead of letting one value set the table width', () => {
        const wide = [{note: new Utf8Value('x'.repeat(500))}];
        const plan = planTable(wide, inferColumns(wide), {maxColumnWidth: 20});
        expect(plan.columns[0].width).toBe(20);
        expect(plan.truncated).toBe(true);
    });

    it('measures only the sampled rows so a stream can be laid out before it ends', () => {
        const streamed = [{v: new Utf8Value('ab')}, {v: new Utf8Value('x'.repeat(50))}];
        const plan = planTable(streamed, inferColumns(streamed), {sample: 1});
        expect(plan.columns[0].width).toBe(2);
    });
});
