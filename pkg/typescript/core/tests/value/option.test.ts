// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Option, Int4Value, NoneValue, Utf8Value, isOption} from '../../src/value';
import {NONE_PRESENTATION} from '../../src/present';
import {ForeignOption, unbrandedOption} from '../foreign-option';

describe('Option', () => {
    describe('state', () => {
        it('a Some is some and not none, a None is none and not some', () => {
            expect(Option.some(1).isSome()).toBe(true);
            expect(Option.some(1).isNone()).toBe(false);
            expect(Option.none('Int4').isSome()).toBe(false);
            expect(Option.none('Int4').isNone()).toBe(true);
        });

        it('a Some over a falsy value is still some', () => {
            expect(Option.some(0).isSome()).toBe(true);
            expect(Option.some('').isSome()).toBe(true);
            expect(Option.some(undefined).isSome()).toBe(true);
            expect(Option.some(null).isSome()).toBe(true);
        });
    });

    describe('unwrap', () => {
        it('returns the value of a Some', () => {
            expect(Option.some(7).unwrap()).toBe(7);
        });

        it('throws on a None naming the missing type', () => {
            expect(() => Option.none('Int4').unwrap()).toThrow('unwrap on a None of type Int4');
            expect(() => Option.none({Option: 'Utf8'}).unwrap()).toThrow('unwrap on a None of type Option(Utf8)');
        });

        it('unwrapOr returns the value of a Some and the fallback for a None', () => {
            expect(Option.some(7).unwrapOr(0)).toBe(7);
            expect(Option.none<number>('Int4').unwrapOr(0)).toBe(0);
        });
    });

    describe('map', () => {
        it('applies the function to a Some', () => {
            expect(Option.some(2).map(n => n * 3).unwrap()).toBe(6);
        });

        it('leaves a None as a None of the same missing type without calling the function', () => {
            let called = false;
            const mapped = Option.none<number>('Int4').map(n => {
                called = true;
                return n * 3;
            });
            expect(called).toBe(false);
            expect(mapped.isNone()).toBe(true);
            expect(mapped.type).toEqual({Option: 'Int4'});
        });

        it('maps through every layer of a nested option', () => {
            const nested = Option.some(Option.some(2));
            const mapped = nested.map(inner => inner.map(n => n + 1));
            expect(mapped.unwrap().unwrap()).toBe(3);
            const someNone = Option.some(Option.none<number>('Int4'));
            const mappedNone = someNone.map(inner => inner.map(n => n + 1));
            expect(mappedNone.isSome()).toBe(true);
            expect(mappedNone.unwrap().isNone()).toBe(true);
        });
    });

    describe('type', () => {
        it('wraps the missing type in one Option layer for a None', () => {
            expect(Option.none('Int4').type).toEqual({Option: 'Int4'});
            expect(Option.none({Option: 'Int4'}).type).toEqual({Option: {Option: 'Int4'}});
        });

        it('wraps the type of a Value or nested Option for a Some', () => {
            expect(Option.some(new Int4Value(1)).type).toEqual({Option: 'Int4'});
            expect(Option.some(Option.none('Utf8')).type).toEqual({Option: {Option: 'Utf8'}});
            expect(Option.some(Option.some(new Utf8Value('a'))).type).toEqual({Option: {Option: 'Utf8'}});
        });

        it('throws for a Some over a raw primitive, since no wire type can be read off it', () => {
            expect(() => Option.some(1).type).toThrow('Option.some over a raw number has no wire type');
        });
    });

    describe('isOption', () => {
        it('accepts a Some and a None of this module', () => {
            expect(isOption(Option.some(1))).toBe(true);
            expect(isOption(Option.none('Int4'))).toBe(true);
        });

        it('accepts an Option from another copy of the module, which instanceof would reject', () => {
            const foreign = ForeignOption.some(1);
            expect(foreign instanceof Option).toBe(false);
            expect(isOption(foreign)).toBe(true);
            expect(isOption(ForeignOption.none('Int4'))).toBe(true);
        });

        it('rejects a NoneValue, a plain object, an unbranded look-alike and non-objects', () => {
            expect(isOption(new NoneValue('Int4'))).toBe(false);
            expect(isOption({})).toBe(false);
            expect(isOption({some: true, value: 1})).toBe(false);
            expect(isOption(unbrandedOption(1))).toBe(false);
            expect(isOption({[Symbol.for('reifydb.option')]: 1})).toBe(false);
            expect(isOption(null)).toBe(false);
            expect(isOption(undefined)).toBe(false);
            expect(isOption(1)).toBe(false);
            expect(isOption('some')).toBe(false);
        });
    });

    describe('presentation', () => {
        it('toString shows the value of a Some and the shared none text for a None', () => {
            expect(Option.some(7).toString()).toBe('7');
            expect(Option.some(new Utf8Value('hi')).toString()).toBe('hi');
            expect(Option.none('Int4').toString()).toBe(NONE_PRESENTATION.text);
            expect(Option.some(Option.none('Int4')).toString()).toBe(NONE_PRESENTATION.text);
            expect(`${Option.some(Option.some(3))}`).toBe('3');
        });

        it('toJSON yields the value of a Some and null for a None, nesting through layers', () => {
            expect(Option.some(7).toJSON()).toBe(7);
            expect(Option.none('Int4').toJSON()).toBeNull();
            expect(JSON.stringify({a: Option.some(Option.none('Int4')), b: Option.some(Option.some(2))})).toBe('{"a":null,"b":2}');
            expect(Option.some(new Int4Value(5)).toJSON()).toBe('5');
            expect(JSON.stringify(Option.some(new Int4Value(5)))).toBe('"5"');
            expect(JSON.stringify(Option.some(Option.some(new Int4Value(undefined))))).toBe('null');
        });
    });
});
