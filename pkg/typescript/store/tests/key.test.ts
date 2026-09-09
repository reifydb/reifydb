// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {entryKey} from '../src';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items';

describe('entryKey', () => {
    it('same rql, params and shape produce the same key', () => {
        expect(entryKey(rql, {a: 1, b: 'x'}, shape)).toBe(entryKey(rql, {a: 1, b: 'x'}, shape));
    });

    it('params in a different object order produce the same key', () => {
        expect(entryKey(rql, {a: 1, b: 'x'}, shape)).toBe(entryKey(rql, {b: 'x', a: 1}, shape));
    });

    it('different params produce a different key', () => {
        expect(entryKey(rql, {a: 1}, shape)).not.toBe(entryKey(rql, {a: 2}, shape));
    });

    it('different rql produces a different key', () => {
        expect(entryKey(rql, null, shape)).not.toBe(entryKey(rql + ' filter id == 1', null, shape));
    });

    it('a different shape produces a different key', () => {
        const other = Shape.object({id: Shape.int4(), name: Shape.option(Shape.string())});
        expect(entryKey(rql, null, shape)).not.toBe(entryKey(rql, null, other));
    });
});
