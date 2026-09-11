// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {transformResult, transformFrames} from '../../src/shape/transform';
import {ShapeMismatch} from '../../src/shape/check';
import {Shape} from '../../src/shape/builder';
import {columnsToRows} from '../../src/decoder';
import {NONE_VALUE, noneMarker} from '../../src/constant';
import {Int4Value, Int8Value, NoneValue, Option} from '../../src/value';
import {Frame} from '../../src/types';

describe('transformResult with option shapes', () => {
    const primitive = Shape.object({n: Shape.option(Shape.option(Shape.int4()))});
    const value = Shape.object({n: Shape.option(Shape.option(Shape.int4Value()))});

    it('wraps a value in a Some at every layer, coercing under a primitive shape and keeping the Value under a value shape', () => {
        const row = {n: new Int4Value(7)};
        const asPrimitive = transformResult(row, primitive);
        expect(asPrimitive.n).toBeInstanceOf(Option);
        expect(asPrimitive.n.unwrap()).toBeInstanceOf(Option);
        expect(asPrimitive.n.unwrap().unwrap()).toBe(7);
        const asValue = transformResult(row, value);
        expect(asValue.n.unwrap().unwrap()).toBeInstanceOf(Int4Value);
        expect(asValue.n.unwrap().unwrap().valueOf()).toBe(7);
    });

    it('coerces to bigint under a bigint primitive shape inside an option', () => {
        const shape = Shape.object({n: Shape.option(Shape.int8())});
        const out = transformResult({n: new Int8Value(5n)}, shape);
        expect(out.n.unwrap()).toBe(5n);
    });

    it('turns a none whose depth equals the remaining layers into a None at this level', () => {
        const row = {n: new NoneValue({Option: 'Int4'})};
        for (const shape of [primitive, value]) {
            const out = transformResult(row, shape);
            expect(out.n.isNone()).toBe(true);
            expect(out.n.type).toEqual({Option: {Option: 'Int4'}});
        }
    });

    it('turns a shallower none into a Some of a None', () => {
        const row = {n: new NoneValue('Int4')};
        for (const shape of [primitive, value]) {
            const out = transformResult(row, shape);
            expect(out.n.isSome()).toBe(true);
            expect(out.n.unwrap().isNone()).toBe(true);
            expect(out.n.unwrap().type).toEqual({Option: 'Int4'});
        }
    });

    it('rejects a none deeper than the remaining layers as drift', () => {
        const shape = Shape.object({n: Shape.option(Shape.int4())});
        const row = {n: new NoneValue({Option: 'Int4'})};
        expect(() => transformResult(row, shape)).toThrow(ShapeMismatch);
        expect(() => transformResult(row, shape)).toThrow('a none 2 layers deep does not fit a shape with 1 option layers');
    });

    it('handles a depth one option the same way', () => {
        const shape = Shape.object({n: Shape.option(Shape.int4())});
        expect(transformResult({n: new Int4Value(1)}, shape).n.unwrap()).toBe(1);
        const none = transformResult({n: new NoneValue('Int4')}, shape).n;
        expect(none.isNone()).toBe(true);
        expect(none.type).toEqual({Option: 'Int4'});
    });

    it('carries every state of a depth two column from the wire through decode into Option values', () => {
        const frame: Frame = {
            columns: [{name: 'n', type: {Option: {Option: 'Int4'}}, payload: [NONE_VALUE, noneMarker(1), '7']}],
        };
        const [rows] = transformFrames([columnsToRows(frame.columns)], [primitive]);
        expect(rows[0].n.isNone()).toBe(true);
        expect(rows[1].n.isSome()).toBe(true);
        expect(rows[1].n.unwrap().isNone()).toBe(true);
        expect(rows[2].n.unwrap().unwrap()).toBe(7);
    });
});
