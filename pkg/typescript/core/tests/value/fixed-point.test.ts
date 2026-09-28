// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {
    DecimalValue, NoneValue, Option, Type, decimalType, decode, fixedPointTypeName, typeFromWire, typeToWire,
} from '../../src';
import {ShapeMismatch, checkFrame} from '../../src/shape/check';
import {Shape} from '../../src/shape/builder';
import {Frame} from '../../src/types';

describe('fixed-point types', () => {
    it('default to the bare parameters the server assumes when a descriptor omits them', () => {
        // A default that differs from the server's would change the column scale and every rendered value.
        expect(decimalType()).toEqual({Decimal: {precision: 76, scale: 10}});
    });

    it.each([
        [() => decimalType(10, 11), 'scale'],
        [() => decimalType(10, -1), 'scale'],
    ])('rejects out of range parameters (%#)', (build, message) => {
        expect(build).toThrow(message);
    });

    it('names a type the way the server displays it', () => {
        expect(fixedPointTypeName(decimalType())).toBe('Decimal');
        expect(fixedPointTypeName(decimalType(76, 0))).toBe('Decimal(76, 0)');
        expect(fixedPointTypeName(decimalType(38, 9))).toBe('Decimal(38, 9)');
    });
});

describe('fixed-point wire descriptors', () => {
    it('read precision and scale from the descriptor', () => {
        expect(typeFromWire({id: 'Decimal', precision: 12, scale: 2})).toEqual({Decimal: {precision: 12, scale: 2}});
        expect(typeFromWire({id: 'Option', underlying: {id: 'Decimal', precision: 12, scale: 2}}))
            .toEqual({Option: {Decimal: {precision: 12, scale: 2}}});
    });

    it('fill a missing field with the bare default', () => {
        expect(typeFromWire({id: 'Decimal'})).toEqual({Decimal: {precision: 76, scale: 10}});
        expect(typeFromWire({id: 'Decimal', precision: 20})).toEqual({Decimal: {precision: 20, scale: 10}});
    });

    it('reject a descriptor the server would reject', () => {
        expect(() => typeFromWire({id: 'Decimal', precision: 5, scale: 6})).toThrow('scale');
    });

    it('round trip through the wire rendering', () => {
        expect(typeToWire(decimalType(12, 2))).toEqual({id: 'Decimal', precision: 12, scale: 2});
        for (const type of [decimalType(12, 2), {Option: decimalType()}] as Type[]) {
            expect(typeFromWire(typeToWire(type))).toEqual(type);
        }
    });
});

describe('fixed-point columns in core', () => {
    it('decode a decimal cell into a DecimalValue and a none into a NoneValue keeping the parameters', () => {
        const type = decimalType(10, 3);
        const value = decode({type, value: '-0.005'});
        expect(value).toBeInstanceOf(DecimalValue);
        expect(value.toString()).toBe('-0.005');
        const none = decode({type: {Option: type}, value: '⟪none⟫'});
        expect(none).toBeInstanceOf(NoneValue);
        expect((none as NoneValue).innerType).toEqual(type);
    });

    it('reject an unparsable decimal cell', () => {
        expect(() => decode({type: decimalType(), value: 'abc'})).toThrow();
    });

    it('describe the type of a none option', () => {
        expect(() => Option.none(decimalType(38, 9)).unwrap()).toThrow('Decimal(38, 9)');
    });

    it('match a decimal shape whatever the column precision', () => {
        // The shape names the value class, so a parameterized column must still satisfy it.
        const frame: Frame = {columns: [{name: 'd', type: decimalType(38, 9), payload: ['1.000000000']}]};
        expect(() => checkFrame(frame.columns, Shape.object({d: Shape.decimal()}))).not.toThrow();
        expect(() => checkFrame(frame.columns, Shape.object({d: Shape.decimalValue()}))).not.toThrow();
        expect(() => checkFrame(frame.columns, Shape.object({d: Shape.int4()})))
            .toThrow('column "d": expected Int4, got Decimal(38, 9)');
        expect(() => checkFrame(frame.columns, Shape.object({d: Shape.int4()}))).toThrow(ShapeMismatch);
    });
});
