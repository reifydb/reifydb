// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Int4Value, ListValue, Utf8Value} from '../../src';

describe('ListValue', () => {
    describe('constructor', () => {
        it('should create instance with a non-empty array', () => {
            const list = new ListValue([new Int4Value(1), new Int4Value(2)]);
            expect(list.items).toEqual([new Int4Value(1), new Int4Value(2)]);
        });

        it('should reject a non-array', () => {
            expect(() => new ListValue({} as any)).toThrow('List value must be an array, got object');
        });

        it('should reject an empty array without an explicit element type', () => {
            expect(() => new ListValue([])).toThrow('An empty List has no element type to infer');
        });

        it('should accept an empty array with an explicit element type', () => {
            const list = new ListValue([], 'Int4');
            expect(list.items).toEqual([]);
        });
    });

    describe('type', () => {
        it('should infer the element type from the first item', () => {
            const list = new ListValue([new Int4Value(1), new Int4Value(2)]);
            expect(list.type).toEqual({List: 'Int4'});
        });

        it('should use the explicit element type for an empty list', () => {
            const list = new ListValue([], 'Utf8');
            expect(list.type).toEqual({List: 'Utf8'});
        });
    });

    describe('equals', () => {
        it('should be equal for the same items in the same order', () => {
            const a = new ListValue([new Int4Value(1), new Int4Value(2)]);
            const b = new ListValue([new Int4Value(1), new Int4Value(2)]);
            expect(a.equals(b)).toBe(true);
        });

        it('should not be equal for items in a different order', () => {
            const a = new ListValue([new Int4Value(1), new Int4Value(2)]);
            const b = new ListValue([new Int4Value(2), new Int4Value(1)]);
            expect(a.equals(b)).toBe(false);
        });

        it('should not be equal for a different length', () => {
            const a = new ListValue([new Int4Value(1)]);
            const b = new ListValue([new Int4Value(1), new Int4Value(2)]);
            expect(a.equals(b)).toBe(false);
        });

        it('should not be equal to a non-ListValue', () => {
            const a = new ListValue([new Int4Value(1)]);
            expect(a.equals(new Int4Value(1))).toBe(false);
        });
    });

    describe('toString', () => {
        it('should render items comma-separated in brackets', () => {
            const list = new ListValue([new Utf8Value('us'), new Utf8Value('eu')]);
            expect(list.toString()).toBe('[us, eu]');
        });
    });

    describe('toJSON', () => {
        it('should recurse into each item toJSON', () => {
            const list = new ListValue([new Utf8Value('us'), new Utf8Value('eu')]);
            expect(JSON.stringify(list)).toBe('["us","eu"]');
        });
    });

    describe('encode', () => {
        it('should encode as a List type with a real JSON array of item values', () => {
            const list = new ListValue([new Int4Value(1), new Int4Value(2)]);
            expect(list.encode()).toEqual({type: {List: 'Int4'}, value: ['1', '2']});
        });
    });
});
