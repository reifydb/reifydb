// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '../../src/shape/builder';
import {ShapeMismatch} from '../../src/shape/check';
import {shapeType} from '../../src/shape/type';

describe('shapeType', () => {
    it('gives the wire type of primitive, value and nested option shapes', () => {
        expect(shapeType(Shape.int4())).toBe('Int4');
        expect(shapeType(Shape.utf8Value())).toBe('Utf8');
        expect(shapeType(Shape.option(Shape.option(Shape.int8())))).toEqual({Option: {Option: 'Int8'}});
    });

    it('throws for object and array shapes, since a None typed from them would carry no real wire type', () => {
        expect(() => shapeType(Shape.object({id: Shape.int4()}))).toThrow(ShapeMismatch);
        expect(() => shapeType(Shape.object({id: Shape.int4()}))).toThrow('a object shape has no wire type');
        expect(() => shapeType(Shape.array(Shape.int4()))).toThrow('a array shape has no wire type');
    });
});
