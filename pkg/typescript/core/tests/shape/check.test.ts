// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {ShapeMismatch, checkFrame, checkFrames} from '../../src/shape/check';
import {Shape} from '../../src/shape/builder';
import {transformFrames} from '../../src/shape/transform';
import {columnsToRows} from '../../src/decoder';
import {Column, Frame} from '../../src/types';

const usersShape = Shape.object({id: Shape.int4(), name: Shape.utf8()});

const users: Frame = {
    columns: [
        {name: 'id', type: 'Int4', payload: ['1']},
        {name: 'name', type: 'Utf8', payload: ['alice']},
    ],
};

describe('checkFrames', () => {
    it('lets a frame whose columns match the shape decode as before', () => {
        expect(() => checkFrames([users], [usersShape])).not.toThrow();
        const rows = transformFrames([columnsToRows(users.columns)], [usersShape]);
        expect(rows).toEqual([[{id: 1, name: 'alice'}]]);
    });

    it('throws when a column has a different type, naming the column, expected and actual type', () => {
        const drifted: Frame = {
            columns: [
                {name: 'id', type: 'Utf8', payload: ['1']},
                {name: 'name', type: 'Utf8', payload: ['alice']},
            ],
        };
        expect(() => checkFrames([drifted], [usersShape])).toThrow(ShapeMismatch);
        expect(() => checkFrames([drifted], [usersShape])).toThrow('column "id": expected Int4, got Utf8');
    });

    it('throws when the shape declares a column the frame lacks', () => {
        const withoutName: Frame = {columns: [{name: 'id', type: 'Int4', payload: ['1']}]};
        expect(() => checkFrames([withoutName], [usersShape])).toThrow(ShapeMismatch);
        expect(() => checkFrames([withoutName], [usersShape])).toThrow('column "name": expected Utf8, missing from frame');
    });

    it('throws when an Option column meets a non-optional shape, since None rows would decode as garbage', () => {
        const nullable: Frame = {
            columns: [
                {name: 'id', type: {Option: 'Int4'}, payload: ['1']},
                {name: 'name', type: 'Utf8', payload: ['alice']},
            ],
        };
        expect(() => checkFrames([nullable], [usersShape])).toThrow(ShapeMismatch);
        expect(() => checkFrames([nullable], [usersShape])).toThrow('column "id": expected Int4, got Option(Int4)');
    });

    it('accepts an Option column under an option shape', () => {
        const shape = Shape.object({id: Shape.option(Shape.int4())});
        const nullable: Frame = {columns: [{name: 'id', type: {Option: 'Int4'}, payload: ['1']}]};
        expect(() => checkFrames([nullable], [shape])).not.toThrow();
    });

    it('throws when a plain column meets an option shape, since the shape promises an Option at that level', () => {
        const shape = Shape.object({id: Shape.option(Shape.int4())});
        expect(() => checkFrames([users], [shape])).toThrow(ShapeMismatch);
        expect(() => checkFrames([users], [shape])).toThrow('column "id": expected Option(Int4), got Int4');
    });

    it('checks the inner type of an option shape against an Option column', () => {
        const shape = Shape.object({id: Shape.option(Shape.int4())});
        const drifted: Frame = {columns: [{name: 'id', type: {Option: 'Utf8'}, payload: ['1']}]};
        expect(() => checkFrames([drifted], [shape])).toThrow('column "id": expected Option(Int4), got Option(Utf8)');
    });

    it('requires the option depth to match exactly in both directions', () => {
        const one = Shape.object({id: Shape.option(Shape.int4())});
        const two = Shape.object({id: Shape.option(Shape.option(Shape.int4()))});
        const depthOne: Frame = {columns: [{name: 'id', type: {Option: 'Int4'}, payload: ['1']}]};
        const depthTwo: Frame = {columns: [{name: 'id', type: {Option: {Option: 'Int4'}}, payload: ['1']}]};
        expect(() => checkFrames([depthTwo], [two])).not.toThrow();
        expect(() => checkFrames([depthOne], [two])).toThrow('column "id": expected Option(Option(Int4)), got Option(Int4)');
        expect(() => checkFrames([depthTwo], [one])).toThrow('column "id": expected Option(Int4), got Option(Option(Int4))');
    });

    it('rejects an Option column whose base type is a sibling under a value shape inside an option', () => {
        const shape = Shape.object({id: Shape.option(Shape.int4Value())});
        const drifted: Frame = {columns: [{name: 'id', type: {Option: 'Int8'}, payload: ['1']}]};
        expect(() => checkFrames([drifted], [shape])).toThrow('column "id": expected Option(Int4), got Option(Int8)');
    });

    it('lets a None shape under an option accept any deeper Option column, since only nones can come out of it', () => {
        const shape = Shape.object({id: Shape.option(Shape.noneValue())});
        const deep: Frame = {columns: [{name: 'id', type: {Option: {Option: 'Utf8'}}, payload: ['']}]};
        expect(() => checkFrames([deep], [shape])).not.toThrow();
        const plain: Frame = {columns: [{name: 'id', type: 'None', payload: ['']}]};
        expect(() => checkFrames([plain], [shape])).toThrow('column "id": expected Option(None), got None');
    });

    it('ignores a frame column the shape does not mention, since shapes are projections', () => {
        const shape = Shape.object({id: Shape.int4()});
        expect(() => checkFrames([users], [shape])).not.toThrow();
    });

    it('throws when there are more frames than shapes', () => {
        expect(() => checkFrames([users, users], [usersShape])).toThrow(ShapeMismatch);
        expect(() => checkFrames([users, users], [usersShape])).toThrow('expected 1 frames for 1 shapes, got 2');
    });

    it('throws when there are fewer frames than shapes', () => {
        expect(() => checkFrames([], [usersShape])).toThrow('expected 1 frames for 1 shapes, got 0');
    });

    it('ignores every frame when the shapes array is empty, since the caller ignores the output', () => {
        expect(() => checkFrames([users, users], [])).not.toThrow();
    });

    it('lets a numeric primitive shape accept every numeric wire type, since the decoder yields a number or bigint for all of them', () => {
        const numeric = ['Int1', 'Int2', 'Int4', 'Int8', 'Int16', 'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16', 'Float4', 'Float8'];
        for (const type of numeric) {
            const frame: Frame = {columns: [{name: 'n', type: type as Column['type'], payload: ['1']}]};
            expect(() => checkFrames([frame], [Shape.object({n: Shape.number()})])).not.toThrow();
            expect(() => checkFrames([frame], [Shape.object({n: Shape.int8()})])).not.toThrow();
        }
    });

    it('rejects a text column under a numeric primitive shape', () => {
        const text: Frame = {columns: [{name: 'n', type: 'Utf8', payload: ['1']}]};
        expect(() => checkFrames([text], [Shape.object({n: Shape.number()})])).toThrow('column "n": expected Float8, got Utf8');
        expect(() => checkFrames([text], [Shape.object({n: Shape.int8()})])).toThrow('column "n": expected Int8, got Utf8');
    });

    it('lets text, temporal and boolean shapes accept their own family only', () => {
        const at: Frame = {columns: [{name: 'at', type: 'DateTime', payload: ['2024-03-15T14:30:00.123Z']}]};
        expect(() => checkFrames([at], [Shape.object({at: Shape.time()})])).not.toThrow();
        expect(() => checkFrames([at], [Shape.object({at: Shape.string()})])).toThrow('column "at": expected Utf8, got DateTime');
        const label: Frame = {columns: [{name: 'label', type: 'Utf8', payload: ['x']}]};
        expect(() => checkFrames([label], [Shape.object({label: Shape.decimal()})])).not.toThrow();
        expect(() => checkFrames([label], [Shape.object({label: Shape.boolean()})])).toThrow('column "label": expected Boolean, got Utf8');
    });

    it('lets a value shape accept a narrower numeric type, since the requested type can hold every value of it', () => {
        const shape = Shape.object({id: Shape.int4Value(), name: Shape.utf8Value()});
        expect(() => checkFrames([users], [shape])).not.toThrow();
        const wider = Shape.object({id: Shape.int8Value()});
        expect(() => checkFrames([users], [wider])).not.toThrow();
    });

    it('rejects a wider numeric type under a value shape, since the requested type would drop range', () => {
        const narrower = Shape.object({id: Shape.int2Value()});
        expect(() => checkFrames([users], [narrower])).toThrow('column "id": expected Int2, got Int4');
    });

    it('rejects a value shape whose type crosses a family, since widening never changes what a value means', () => {
        // Both directions: a float target does not accept an integer column even though the two share a
        // family for primitive shapes, and an unsigned target never accepts a signed column.
        expect(() => checkFrames([users], [Shape.object({id: Shape.float8Value()})]))
            .toThrow('column "id": expected Float8, got Int4');
        expect(() => checkFrames([users], [Shape.object({id: Shape.uint8Value()})]))
            .toThrow('column "id": expected Uint8, got Int4');
        expect(() => checkFrames([users], [Shape.object({id: Shape.utf8Value()})]))
            .toThrow('column "id": expected Utf8, got Int4');
    });

    it('takes a signed value shape for an unsigned column only when it is strictly wider, since Uint1 reaches 255', () => {
        const counter: Frame = {columns: [{name: 'id', type: 'Uint1', payload: ['255']}]};
        expect(() => checkFrames([counter], [Shape.object({id: Shape.int1Value()})]))
            .toThrow('column "id": expected Int1, got Uint1');
        expect(() => checkFrames([counter], [Shape.object({id: Shape.int2Value()})])).not.toThrow();
    });

    it('lets a None shape accept an Option column, since a none payload decodes to NoneValue whatever the inner type', () => {
        const none: Frame = {columns: [{name: 'result', type: {Option: 'Int4'}, payload: ['']}]};
        expect(() => checkFrames([none], [Shape.object({result: Shape.noneValue()})])).not.toThrow();
        expect(() => checkFrames([none], [Shape.object({result: Shape.none()})])).not.toThrow();
        const plain: Frame = {columns: [{name: 'result', type: 'Int4', payload: ['1']}]};
        expect(() => checkFrames([plain], [Shape.object({result: Shape.noneValue()})])).toThrow('column "result": expected None, got Int4');
    });

    it('names the frame shape kind when it is not an object, since a frame is a set of named columns', () => {
        expect(() => checkFrames([users], [Shape.int4()])).toThrow('a frame needs an object shape, got primitive');
    });

    it('rejects nested object and array shapes, since no column type carries a structure', () => {
        const nested = Shape.object({id: Shape.object({inner: Shape.int4()})});
        expect(() => checkFrames([users], [nested])).toThrow('column "id": expected object, got Int4');
        const list = Shape.object({id: Shape.array(Shape.int4())});
        expect(() => checkFrames([users], [list])).toThrow('column "id": expected array, got Int4');
    });
});

describe('checkFrame', () => {
    it('checks a single frame against a single shape for subscription frames', () => {
        const columns: Column[] = [{name: 'id', type: 'Int4', payload: ['1']}];
        expect(() => checkFrame(columns, Shape.object({id: Shape.int4()}))).not.toThrow();
        expect(() => checkFrame(columns, Shape.object({id: Shape.utf8()}))).toThrow('column "id": expected Utf8, got Int4');
    });
});
