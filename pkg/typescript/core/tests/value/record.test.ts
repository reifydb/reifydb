// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Int4Value, ListValue, NoneValue, RecordValue, Utf8Value} from '../../src';

function region(): RecordValue {
    return new RecordValue({id: new Utf8Value('us'), label: new Utf8Value('US')});
}

describe('RecordValue', () => {
    describe('constructor', () => {
        it('should create instance with a plain object of fields', () => {
            const record = region();
            expect(record.fields.id).toEqual(new Utf8Value('us'));
            expect(record.fields.label).toEqual(new Utf8Value('US'));
        });

        it('should reject a non-object', () => {
            expect(() => new RecordValue('x' as any)).toThrow('Record value must be a plain object of fields, got string');
        });

        it('should reject an array', () => {
            expect(() => new RecordValue([] as any)).toThrow('Record value must be a plain object of fields');
        });
    });

    describe('type', () => {
        it('should list fields in insertion order with their value types', () => {
            const record = region();
            expect(record.type).toEqual({Record: [{name: 'id', type: 'Utf8'}, {name: 'label', type: 'Utf8'}]});
        });
    });

    describe('equals', () => {
        it('should be equal for the same fields in the same order', () => {
            expect(region().equals(region())).toBe(true);
        });

        it('should not be equal for fields in a different order', () => {
            const a = new RecordValue({id: new Utf8Value('us'), label: new Utf8Value('US')});
            const b = new RecordValue({label: new Utf8Value('US'), id: new Utf8Value('us')});
            expect(a.equals(b)).toBe(false);
        });

        it('should not be equal for a different field value', () => {
            const a = region();
            const b = new RecordValue({id: new Utf8Value('eu'), label: new Utf8Value('EU')});
            expect(a.equals(b)).toBe(false);
        });

        it('should not be equal to a non-RecordValue', () => {
            expect(region().equals(new Int4Value(1))).toBe(false);
        });
    });

    describe('toString', () => {
        it('should render fields as name: value pairs', () => {
            expect(region().toString()).toBe('{id: us, label: US}');
        });
    });

    describe('toJSON', () => {
        it('should recurse into each field toJSON', () => {
            expect(JSON.stringify(region())).toBe('{"id":"us","label":"US"}');
        });
    });

    describe('encode', () => {
        it('should encode as a Record type with a real JSON object of field values', () => {
            expect(region().encode()).toEqual({
                type: {Record: [{name: 'id', type: 'Utf8'}, {name: 'label', type: 'Utf8'}]},
                value: {id: 'us', label: 'US'},
            });
        });
    });

    describe('none fields', () => {
        it('types a none field as its option, not None, so the server can decode it', () => {
            // The server has no None type id; a record param carrying one is refused outright.
            const record = new RecordValue({id: new Int4Value(1), note: new NoneValue('Utf8')});
            expect(record.type).toEqual({Record: [{name: 'id', type: 'Int4'}, {name: 'note', type: {Option: 'Utf8'}}]});
            expect(record.encode().type).toEqual(record.type);
        });

        it('carries the option type into a list of records built without an element type', () => {
            // A list param takes its element type from the first record, so a None there refuses the whole list.
            const list = new ListValue([new RecordValue({note: new NoneValue('Utf8')})]);
            expect(list.encode().type).toEqual({List: {Record: [{name: 'note', type: {Option: 'Utf8'}}]}});
        });
    });
});
