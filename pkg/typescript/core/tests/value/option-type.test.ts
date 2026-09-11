// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {isOptionType, unwrapOptionType, optionDepth, innerOfOption, BaseType, OptionType, Type} from '../../src/value';

describe('isOptionType', () => {
    it('should return false for BaseType string', () => {
        const baseTypes: BaseType[] = [
            'Blob', 'Boolean', 'Decimal', 'Float4', 'Float8',
            'Int1', 'Int2', 'Int4', 'Int8', 'Int16',
            'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16',
            'Utf8', 'Date', 'DateTime', 'Time', 'Duration',
            'Uuid4', 'Uuid7', 'IdentityId', 'None'
        ];
        for (const t of baseTypes) {
            expect(isOptionType(t)).toBe(false);
        }
    });

    it('should return true for {Option: BaseType}', () => {
        const opt: OptionType = {Option: 'Int4'};
        expect(isOptionType(opt)).toBe(true);
    });

    it('should return true for nested {Option: {Option: BaseType}}', () => {
        const nested: Type = {Option: {Option: 'Int4'}};
        expect(isOptionType(nested)).toBe(true);
    });

    it('should return true for {Option: "Boolean"}', () => {
        expect(isOptionType({Option: 'Boolean'})).toBe(true);
    });

    it('should return true for {Option: "Utf8"}', () => {
        expect(isOptionType({Option: 'Utf8'})).toBe(true);
    });
});

describe('unwrapOptionType', () => {
    it('should return BaseType unchanged', () => {
        const baseTypes: BaseType[] = [
            'Blob', 'Boolean', 'Decimal', 'Float4', 'Float8',
            'Int1', 'Int2', 'Int4', 'Int8', 'Int16',
            'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16',
            'Utf8', 'Date', 'DateTime', 'Time', 'Duration',
            'Uuid4', 'Uuid7', 'IdentityId', 'None'
        ];
        for (const t of baseTypes) {
            expect(unwrapOptionType(t)).toBe(t);
        }
    });

    it('should unwrap {Option: "Int4"} to "Int4"', () => {
        expect(unwrapOptionType({Option: 'Int4'})).toBe('Int4');
    });

    it('should unwrap {Option: "Boolean"} to "Boolean"', () => {
        expect(unwrapOptionType({Option: 'Boolean'})).toBe('Boolean');
    });

    it('should unwrap {Option: "Utf8"} to "Utf8"', () => {
        expect(unwrapOptionType({Option: 'Utf8'})).toBe('Utf8');
    });

    it('should recursively unwrap nested {Option: {Option: "Int4"}} to "Int4"', () => {
        expect(unwrapOptionType({Option: {Option: 'Int4'}})).toBe('Int4');
    });

    it('should recursively unwrap deeply nested options', () => {
        const deep: Type = {Option: {Option: {Option: 'Utf8'}}};
        expect(unwrapOptionType(deep)).toBe('Utf8');
    });
});

describe('optionDepth', () => {
    it('is zero for a base type', () => {
        expect(optionDepth('Int4')).toBe(0);
        expect(optionDepth('None')).toBe(0);
    });

    it('counts every Option layer', () => {
        expect(optionDepth({Option: 'Int4'})).toBe(1);
        expect(optionDepth({Option: {Option: 'Int4'}})).toBe(2);
        expect(optionDepth({Option: {Option: {Option: 'Utf8'}}})).toBe(3);
    });
});

describe('innerOfOption', () => {
    it('removes exactly one layer, keeping the rest nested', () => {
        expect(innerOfOption({Option: 'Int4'})).toBe('Int4');
        expect(innerOfOption({Option: {Option: 'Int4'}})).toEqual({Option: 'Int4'});
    });
});
