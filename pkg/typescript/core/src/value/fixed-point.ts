// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {DecimalType, FixedPointKind, FixedPointType, IntType, Type, UintType} from '.';

export const FIXED_POINT_MAX_PRECISION = 76;
export const FIXED_POINT_NARROW_PRECISION = 38;
export const DECIMAL_DEFAULT_SCALE = 10;

function checkPrecision(kind: FixedPointKind, precision: number): void {
    if (!Number.isInteger(precision) || precision < 1 || precision > FIXED_POINT_MAX_PRECISION) {
        throw new Error(`${kind} precision must be a whole number between 1 and ${FIXED_POINT_MAX_PRECISION}, got ${precision}`);
    }
}

export function intType(precision: number = FIXED_POINT_MAX_PRECISION): IntType {
    checkPrecision('Int', precision);
    return {Int: {precision}};
}

export function uintType(precision: number = FIXED_POINT_MAX_PRECISION): UintType {
    checkPrecision('Uint', precision);
    return {Uint: {precision}};
}

export function decimalType(precision: number = FIXED_POINT_MAX_PRECISION, scale: number = DECIMAL_DEFAULT_SCALE): DecimalType {
    checkPrecision('Decimal', precision);
    if (!Number.isInteger(scale) || scale < 0 || scale > precision) {
        throw new Error(`Decimal scale must be a whole number between 0 and the precision ${precision}, got ${scale}`);
    }
    return {Decimal: {precision, scale}};
}

export function fixedPointType(kind: FixedPointKind, precision: number, scale: number): FixedPointType {
    switch (kind) {
        case 'Decimal':
            return decimalType(precision, scale);
        case 'Int':
        case 'Uint':
            if (scale !== 0) {
                throw new Error(`${kind} carries scale ${scale} but must have scale 0`);
            }
            return kind === 'Int' ? intType(precision) : uintType(precision);
    }
}

export function isFixedPointType(t: Type): t is FixedPointType {
    return typeof t === 'object' && t !== null && ('Int' in t || 'Uint' in t || 'Decimal' in t);
}

export function fixedPointKind(t: FixedPointType): FixedPointKind {
    if ('Decimal' in t) return 'Decimal';
    return 'Int' in t ? 'Int' : 'Uint';
}

export function fixedPointPrecision(t: FixedPointType): number {
    if ('Decimal' in t) return t.Decimal.precision;
    return 'Int' in t ? t.Int.precision : t.Uint.precision;
}

export function fixedPointScale(t: FixedPointType): number {
    return 'Decimal' in t ? t.Decimal.scale : 0;
}

export function fixedPointTypeName(t: FixedPointType): string {
    const kind = fixedPointKind(t);
    const precision = fixedPointPrecision(t);
    if (kind === 'Decimal') {
        const scale = fixedPointScale(t);
        return precision === FIXED_POINT_MAX_PRECISION && scale === DECIMAL_DEFAULT_SCALE
            ? kind
            : `${kind}(${precision}, ${scale})`;
    }
    return precision === FIXED_POINT_MAX_PRECISION ? kind : `${kind}(${precision})`;
}
