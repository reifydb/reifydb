// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {
    DecimalValue, NoneValue, Option, Type, decimalType, decode, fixedPointTypeName, intType, typeFromWire, typeToWire,
    uintType,
} from '../../src';
import {ShapeMismatch, checkFrame} from '../../src/shape/check';
import {Shape} from '../../src/shape/builder';
import {Frame} from '../../src/types';

describe('fixed-point types', () => {
    it('default to the bare parameters the server assumes when a descriptor omits them', () => {
        // A default that differs from the server's would change the column scale and every rendered value.
        expect(intType()).toEqual({Int: {precision: 76}});
        expect(uintType()).toEqual({Uint: {precision: 76}});
        expect(decimalType()).toEqual({Decimal: {precision: 76, scale: 10}});
    });

    it.each([
        [() => intType(0), 'precision'],
        [() => intType(77), 'precision'],
        [() => uintType(1.5), 'precision'],
        [() => decimalType(10, 11), 'scale'],
        [() => decimalType(10, -1), 'scale'],
    ])('rejects out of range parameters (%#)', (build, message) => {
        expect(build).toThrow(message);
    });

    it('names a type the way the server displays it', () => {
        expect(fixedPointTypeName(intType())).toBe('Int');
        expect(fixedPointTypeName(intType(38))).toBe('Int(38)');
        expect(fixedPointTypeName(uintType(5))).toBe('Uint(5)');
        expect(fixedPointTypeName(decimalType())).toBe('Decimal');
        expect(fixedPointTypeName(decimalType(76, 0))).toBe('Decimal(76, 0)');
        expect(fixedPointTypeName(decimalType(38, 9))).toBe('Decimal(38, 9)');
    });
});

describe('fixed-point wire descriptors', () => {
    it('read precision and scale from the descriptor', () => {
        expect(typeFromWire({id: 'Int', precision: 38})).toEqual({Int: {precision: 38}});
        expect(typeFromWire({id: 'Uint', precision: 1})).toEqual({Uint: {precision: 1}});
        expect(typeFromWire({id: 'Decimal', precision: 12, scale: 2})).toEqual({Decimal: {precision: 12, scale: 2}});
        expect(typeFromWire({id: 'Option', underlying: {id: 'Decimal', precision: 12, scale: 2}}))
            .toEqual({Option: {Decimal: {precision: 12, scale: 2}}});
    });

    it('fill a missing field with the bare default', () => {
        expect(typeFromWire({id: 'Int'})).toEqual({Int: {precision: 76}});
        expect(typeFromWire({id: 'Decimal'})).toEqual({Decimal: {precision: 76, scale: 10}});
        expect(typeFromWire({id: 'Decimal', precision: 20})).toEqual({Decimal: {precision: 20, scale: 10}});
    });

    it('reject a descriptor the server would reject', () => {
        expect(() => typeFromWire({id: 'Int', precision: 77})).toThrow('precision');
        expect(() => typeFromWire({id: 'Decimal', precision: 5, scale: 6})).toThrow('scale');
    });

    it('round trip through the wire rendering', () => {
        // An int descriptor must carry no scale field, the server never writes one.
        expect(typeToWire(intType(38))).toEqual({id: 'Int', precision: 38});
        expect(typeToWire(decimalType(12, 2))).toEqual({id: 'Decimal', precision: 12, scale: 2});
        for (const type of [intType(38), uintType(), decimalType(12, 2), {Option: decimalType()}] as Type[]) {
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

    it('report int and uint cells as unsupported instead of guessing a value class', () => {
        expect(() => decode({type: intType(38), value: '1'})).toThrow('Unsupported type: Int(38)');
        expect(() => decode({type: uintType(), value: '1'})).toThrow('Unsupported type: Uint');
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
