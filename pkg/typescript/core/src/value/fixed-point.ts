// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {DecimalType, FixedPointKind, FixedPointType, Type} from '.';

export const FIXED_POINT_MAX_PRECISION = 76;
export const FIXED_POINT_NARROW_PRECISION = 38;
export const DECIMAL_DEFAULT_SCALE = 10;

function checkPrecision(kind: FixedPointKind, precision: number): void {
    if (!Number.isInteger(precision) || precision < 1 || precision > FIXED_POINT_MAX_PRECISION) {
        throw new Error(`${kind} precision must be a whole number between 1 and ${FIXED_POINT_MAX_PRECISION}, got ${precision}`);
    }
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
    }
}

export function isFixedPointType(t: Type): t is FixedPointType {
    return typeof t === 'object' && t !== null && 'Decimal' in t;
}

export function fixedPointKind(t: FixedPointType): FixedPointKind {
    return 'Decimal';
}

export function fixedPointPrecision(t: FixedPointType): number {
    return t.Decimal.precision;
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
