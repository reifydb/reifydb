// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {encodeParams, encodeValue} from '../src/encoder';
import {decode} from '../src/decoder';
import {NONE_VALUE, noneMarker} from '../src/constant';
import {Int4Value, NoneValue, Option, Utf8Value} from '../src/value';
import {ForeignOption, unbrandedOption} from './foreign-option';

describe('encodeParams', () => {
    it('rejects null and undefined naming the parameter, at the top level and inside arrays and objects', () => {
        expect(() => encodeValue(null)).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
        expect(() => encodeValue(undefined)).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
        expect(() => encodeParams([1, null])).toThrow('parameter $2 is null or undefined, use Option.none(inner)');
        expect(() => encodeParams([undefined])).toThrow('parameter $1 is null or undefined, use Option.none(inner)');
        expect(() => encodeParams({a: 1, name: null})).toThrow('parameter $name is null or undefined, use Option.none(inner)');
        expect(() => encodeParams({name: undefined})).toThrow('parameter $name is null or undefined, use Option.none(inner)');
        expect(() => encodeParams({name: Option.some(null)})).toThrow('parameter $name is null or undefined, use Option.none(inner)');
    });

    it('encodes a typed NoneValue like Option.none of its inner type and rejects an untyped one naming the parameter', () => {
        expect(encodeParams([new NoneValue('Int4')])).toEqual(encodeParams([Option.none('Int4')]));
        expect(() => encodeParams([1, new NoneValue()])).toThrow('parameter $2 is a NoneValue without an inner type, use Option.none(inner)');
        expect(() => encodeParams({name: new NoneValue()})).toThrow('parameter $name is a NoneValue without an inner type, use Option.none(inner)');
    });

    it('encodes a Some as its value with the type wrapped in one Option layer', () => {
        expect(encodeParams([Option.some(new Int4Value(7))])).toEqual([{type: {id: 'Option', underlying: {id: 'Int4'}}, value: '7'}]);
        expect(encodeParams({name: Option.some('alice')})).toEqual({name: {type: {id: 'Option', underlying: {id: 'Utf8'}}, value: 'alice'}});
    });

    // encodeValue keeps the client's own type representation; only encodeParams renders it for the
    // wire, so these stay in the {Option: ...} form the rest of the client reads.
    it('encodes an Option from another copy of the module the same as one of its own', () => {
        expect(encodeValue(ForeignOption.some(new Int4Value(7)))).toEqual(encodeValue(Option.some(new Int4Value(7))));
        expect(encodeValue(ForeignOption.none('Int4'))).toEqual(encodeValue(Option.none('Int4')));
        expect(encodeValue(ForeignOption.some(ForeignOption.none('Utf8')))).toEqual({type: {Option: {Option: 'Utf8'}}, value: noneMarker(1)});
    });

    it('rejects an unbranded object shaped like an Option as it rejects any other object', () => {
        expect(() => encodeValue(unbrandedOption(new Int4Value(7)))).toThrow('Cannot encode value of type object');
    });

    it('encodes a None as a typed none with the Option type and the none marker', () => {
        expect(encodeParams([Option.none('Int4')])).toEqual([{type: {id: 'Option', underlying: {id: 'Int4'}}, value: NONE_VALUE}]);
        expect(encodeParams({name: Option.none({Option: 'Utf8'})})).toEqual({name: {type: {id: 'Option', underlying: {id: 'Option', underlying: {id: 'Utf8'}}}, value: NONE_VALUE}});
    });

    it('wraps nested options layer by layer and pushes a none marker one Some deeper per layer', () => {
        expect(encodeValue(Option.some(Option.some(new Int4Value(7))))).toEqual({type: {Option: {Option: 'Int4'}}, value: '7'});
        expect(encodeValue(Option.some(Option.none('Int4')))).toEqual({type: {Option: {Option: 'Int4'}}, value: noneMarker(1)});
        expect(encodeValue(Option.some(Option.some(Option.none('Utf8'))))).toEqual({type: {Option: {Option: {Option: 'Utf8'}}}, value: noneMarker(2)});
        expect(encodeValue(Option.none({Option: 'Int4'}))).toEqual({type: {Option: {Option: 'Int4'}}, value: NONE_VALUE});
    });

    it('produces payloads the decoder reads back at the same layer', () => {
        const someNone = decode(encodeValue(Option.some(Option.none('Int4'))));
        expect(someNone).toBeInstanceOf(NoneValue);
        expect((someNone as NoneValue).innerType).toBe('Int4');
        const none = decode(encodeValue(Option.none({Option: 'Int4'})));
        expect((none as NoneValue).innerType).toEqual({Option: 'Int4'});
        const value = decode(encodeValue(Option.some(Option.some(new Utf8Value('x')))));
        expect(value).toBeInstanceOf(Utf8Value);
        expect(value.valueOf()).toBe('x');
    });
});
