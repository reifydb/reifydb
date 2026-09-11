// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {typeFromWire, typeToWire, framesFromWire} from '../src/value';

// The wire names a type by its `id` and nests what it wraps under `underlying`. The client keeps its own
// representation, so these two functions are the only crossing point; a descriptor that leaked past them
// would make isOptionType read false on an option and decode the column as a base type.

describe('typeToWire', () => {
    it('renders a base type as an object naming it, never as a bare string', () => {
        expect(typeToWire('Int4')).toEqual({id: 'Int4'});
        expect(typeToWire('Utf8')).toEqual({id: 'Utf8'});
    });

    it('gives a base type no underlying key at all', () => {
        expect('underlying' in typeToWire('Int4')).toBe(false);
    });

    it('names Option as the id and carries the wrapped type underneath', () => {
        expect(typeToWire({Option: 'Int4'})).toEqual({id: 'Option', underlying: {id: 'Int4'}});
    });

    it('nests one level per Option layer so the none depth stays readable', () => {
        expect(typeToWire({Option: {Option: 'Utf8'}}))
            .toEqual({id: 'Option', underlying: {id: 'Option', underlying: {id: 'Utf8'}}});
    });
});

describe('typeFromWire', () => {
    it('reads a base type back', () => {
        expect(typeFromWire({id: 'Int4'})).toBe('Int4');
    });

    it('reads an option back into the representation the decoder expects', () => {
        expect(typeFromWire({id: 'Option', underlying: {id: 'Utf8'}})).toEqual({Option: 'Utf8'});
    });

    it('round trips every option depth the client can build', () => {
        for (const type of ['Int4', {Option: 'Int4'}, {Option: {Option: 'Utf8'}}, {Option: {Option: {Option: 'Boolean'}}}] as const) {
            expect(typeFromWire(typeToWire(type as any))).toEqual(type);
        }
    });

    it('rejects the old bare-string spelling rather than letting two spellings live', () => {
        expect(() => typeFromWire('Int4' as any)).toThrow('Expected a type descriptor object');
    });

    it('rejects an Option with no underlying instead of inventing one', () => {
        expect(() => typeFromWire({id: 'Option'})).toThrow('missing its underlying type');
    });
});

describe('framesFromWire', () => {
    it('converts every column type and leaves the payload and the other frame keys alone', () => {
        const frames = [{
            row_numbers: [1],
            columns: [
                {name: 'id', type: {id: 'Int4'}, payload: ['1']},
                {name: 'v', type: {id: 'Option', underlying: {id: 'Utf8'}}, payload: ['a']},
            ],
        }];

        expect(framesFromWire(frames)).toEqual([{
            row_numbers: [1],
            columns: [
                {name: 'id', type: 'Int4', payload: ['1']},
                {name: 'v', type: {Option: 'Utf8'}, payload: ['a']},
            ],
        }]);
    });

    it('leaves a frame without columns untouched rather than throwing', () => {
        expect(framesFromWire([{row_numbers: []}])).toEqual([{row_numbers: []}]);
    });
});
