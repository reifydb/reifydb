// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { describe, expect, it } from 'vitest';
import { VectorValue } from '../../src';

describe('VectorValue', () => {
    describe('constructor', () => {
        it('should create instance with Float32Array', () => {
            const value = new VectorValue(new Float32Array([0.5, -1.5]));
            expect(value.value).toEqual(new Float32Array([0.5, -1.5]));
            expect(value.type).toBe('Vector');
        });

        it('should create instance with number array', () => {
            const value = new VectorValue([1, 2, 3]);
            expect(value.value).toEqual(new Float32Array([1, 2, 3]));
        });

        it('should create instance with undefined', () => {
            expect(new VectorValue(undefined).value).toBeUndefined();
        });

        it('should round elements to f32 precision', () => {
            // 0.1 is not representable in f32; the stored value must be the f32 nearest.
            const value = new VectorValue([0.1]);
            expect(value.value![0]).toBe(Math.fround(0.1));
            expect(value.value![0]).not.toBe(0.1);
        });

        it('should throw error for non-numeric element', () => {
            expect(() => new VectorValue(['a'] as unknown as number[])).toThrow();
        });
    });

    describe('parse', () => {
        it('should parse a bracketed list', () => {
            expect(new VectorValue([0.5, -1, 0]).equals(VectorValue.parse('[0.5, -1, 0]'))).toBe(true);
        });

        it('should parse an empty vector', () => {
            expect(VectorValue.parse('[]').dims()).toBe(0);
        });

        it('should parse a single element', () => {
            expect(VectorValue.parse('[42]').value).toEqual(new Float32Array([42]));
        });

        it('should tolerate surrounding whitespace', () => {
            expect(VectorValue.parse('  [1, 2]  ').dims()).toBe(2);
        });

        it('should return none for empty string', () => {
            expect(VectorValue.parse('').value).toBeUndefined();
        });

        it('should throw error for an unbracketed list', () => {
            expect(() => VectorValue.parse('1, 2, 3')).toThrow();
        });

        it('should throw error for a non-numeric element', () => {
            expect(() => VectorValue.parse('[1, abc]')).toThrow();
        });
    });

    describe('dims', () => {
        it('should count elements', () => {
            expect(new VectorValue([1, 2, 3, 4]).dims()).toBe(4);
        });

        it('should be zero for none', () => {
            expect(new VectorValue(undefined).dims()).toBe(0);
        });
    });

    describe('toString', () => {
        it('should render a bracketed list', () => {
            expect(new VectorValue([0.5, -1, 0]).toString()).toBe('[0.5, -1, 0]');
        });

        it('should render an empty vector', () => {
            expect(new VectorValue([]).toString()).toBe('[]');
        });

        it('should render none', () => {
            expect(new VectorValue(undefined).toString()).toBe('none');
        });

        it('should round-trip through parse', () => {
            const original = new VectorValue([0.25, -3.5, 100]);
            expect(VectorValue.parse(original.toString()).equals(original)).toBe(true);
        });
    });

    describe('equals', () => {
        it('should be true for identical vectors', () => {
            expect(new VectorValue([1, 2]).equals(new VectorValue([1, 2]))).toBe(true);
        });

        it('should be false for different elements', () => {
            expect(new VectorValue([1, 2]).equals(new VectorValue([1, 3]))).toBe(false);
        });

        it('should be false for different dimensions', () => {
            expect(new VectorValue([1, 2]).equals(new VectorValue([1, 2, 0]))).toBe(false);
        });

        it('should be false against a different type', () => {
            expect(new VectorValue([1]).equals(new VectorValue(undefined))).toBe(false);
        });

        it('should be true for two nones', () => {
            expect(new VectorValue(undefined).equals(new VectorValue(undefined))).toBe(true);
        });
    });

    describe('toJSON', () => {
        it('should serialize elements as strings', () => {
            expect(new VectorValue([0.5, -1]).toJSON()).toEqual(['0.5', '-1']);
        });

        it('should serialize none as null', () => {
            expect(new VectorValue(undefined).toJSON()).toBeNull();
        });
    });

    describe('encode', () => {
        it('should encode as a Vector type/value pair', () => {
            expect(new VectorValue([1, 2]).encode()).toEqual({ type: 'Vector', value: '[1, 2]' });
        });

        it('should encode none with the none sentinel', () => {
            expect(new VectorValue(undefined).encode().type).toBe('Vector');
        });
    });
});
