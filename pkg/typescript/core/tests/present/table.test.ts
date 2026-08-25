// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {format_value, infer_columns, plan_table, value_role} from '../../src/present';
import {Int4Value, NoneValue, Utf8Value} from '../../src/value';

describe('format_value', () => {
    it('renders a missing value as none, never as undefined or null', () => {
        // reifydb has no null - the shell used to print the literal string "undefined" here
        expect(format_value(null)).toBe('none');
        expect(format_value(undefined)).toBe('none');
        expect(format_value(new NoneValue())).toBe('none');
    });

    it('renders a bigint without throwing', () => {
        expect(format_value(9007199254740993n)).toBe('9007199254740993');
    });
});

describe('value_role', () => {
    it('reads the role from the value type, not the javascript type', () => {
        expect(value_role(new Int4Value(4))).toBe('number');
        expect(value_role(new Utf8Value('4'))).toBe('string');
    });

    it('treats a missing value as none', () => {
        expect(value_role(new NoneValue())).toBe('none');
        expect(value_role(undefined)).toBe('none');
    });
});

describe('infer_columns', () => {
    it('right-aligns numeric columns and left-aligns the rest', () => {
        const columns = infer_columns([{age: new Int4Value(34), name: new Utf8Value('alice')}]);
        expect(columns.map((c) => [c.name, c.align])).toEqual([
            ['age', 'right'],
            ['name', 'left'],
        ]);
    });

    it('skips leading none values when deciding a column type', () => {
        // one missing value at the top must not flip the whole column to left-aligned text
        const columns = infer_columns([
            {age: new NoneValue()},
            {age: new Int4Value(34)},
        ]);
        expect(columns[0].align).toBe('right');
        expect(columns[0].role).toBe('number');
    });

    it('keeps first-seen column order across ragged rows', () => {
        const columns = infer_columns([{a: 1}, {b: 2}, {a: 3, c: 4}]);
        expect(columns.map((c) => c.name)).toEqual(['a', 'b', 'c']);
    });
});

describe('plan_table', () => {
    const rows = [
        {name: new Utf8Value('alice'), role: new Utf8Value('admin')},
        {name: new Utf8Value('bob'), role: new Utf8Value('user')},
    ];

    it('sizes each column to the widest of its header and values', () => {
        const plan = plan_table(rows, infer_columns(rows));
        expect(plan.columns.map((c) => c.width)).toEqual([5, 5]);
    });

    it('reports the columns it had to drop instead of dropping them silently', () => {
        // a dropped column used to vanish with no trace, so a result looked complete when it was not
        const plan = plan_table(rows, infer_columns(rows), {width: 12});
        expect(plan.columns.map((c) => c.column.name)).toEqual(['name']);
        expect(plan.dropped.map((c) => c.name)).toEqual(['role']);
    });

    it('keeps one squeezed column when even the first does not fit', () => {
        const plan = plan_table(rows, infer_columns(rows), {width: 6});
        expect(plan.columns).toHaveLength(1);
        expect(plan.columns[0].width).toBeGreaterThan(0);
        expect(plan.truncated).toBe(true);
    });

    it('fits every column when the width allows it', () => {
        const plan = plan_table(rows, infer_columns(rows), {width: 80});
        expect(plan.dropped).toHaveLength(0);
    });

    it('caps a runaway column instead of letting one value set the table width', () => {
        const wide = [{note: new Utf8Value('x'.repeat(500))}];
        const plan = plan_table(wide, infer_columns(wide), {max_column_width: 20});
        expect(plan.columns[0].width).toBe(20);
        expect(plan.truncated).toBe(true);
    });

    it('measures only the sampled rows so a stream can be laid out before it ends', () => {
        const streamed = [{v: new Utf8Value('ab')}, {v: new Utf8Value('x'.repeat(50))}];
        const plan = plan_table(streamed, infer_columns(streamed), {sample: 1});
        expect(plan.columns[0].width).toBe(2);
    });
});
