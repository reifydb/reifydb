// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {ShapeNode} from '.';
import {ShapeMismatch} from './check';
import {widen} from './widen';
import {DurationValue, NoneValue, Option, TimeValue, noneDepth} from '../value';
import {FrameResults} from '../types';

function snakeToCamel(key: string): string {
    return key.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
}

export function transformFrames<const S extends readonly ShapeNode[]>(
    frames: any[][],
    shapes: S
): FrameResults<S> {
    const transformed: any[][] = frames.map((frame: any[], frameIndex: number) => {
        const frameShape = shapes[frameIndex];
        if (!frameShape) {
            return frame;
        }
        return frame.map((row: any) => transformResult(row, frameShape));
    });
    return transformed as FrameResults<S>;
}

export function transformResult(row: any, resultShape: any): any {
    if (resultShape && resultShape.kind === 'object' && resultShape.properties) {
        const transformedRow: any = {};
        for (const [key, value] of Object.entries(row)) {
            const propertyShape = resultShape.properties[key];
            const outputKey = snakeToCamel(key);
            if (propertyShape && propertyShape.kind === 'primitive') {
                transformedRow[outputKey] = toPrimitive(value, propertyShape.type);
            } else if (propertyShape && propertyShape.kind === 'value') {
                transformedRow[outputKey] = widen(value, propertyShape.type);
            } else {
                transformedRow[outputKey] = propertyShape ? transformResult(value, propertyShape) : value;
            }
        }
        return transformedRow;
    }

    if (resultShape && resultShape.kind === 'primitive') {
        return toPrimitive(row, resultShape.type);
    }

    if (resultShape && resultShape.kind === 'value') {
        return widen(row, resultShape.type);
    }

    if (resultShape && resultShape.kind === 'array') {
        if (Array.isArray(row)) {
            return row.map((item: any) => transformResult(item, resultShape.items));
        }
        return row;
    }

    if (resultShape && resultShape.kind === 'option') {
        if (row instanceof NoneValue) {
            const remaining = optionLayers(resultShape);
            const depth = noneDepth(row);
            if (depth > remaining) {
                throw new ShapeMismatch(`a none ${depth} layers deep does not fit a shape with ${remaining} option layers`);
            }
            if (depth === remaining) {
                return Option.none(row.innerType);
            }
        }
        return Option.some(transformResult(row, resultShape.inner));
    }

    return row;
}

function optionLayers(shape: ShapeNode): number {
    return shape.kind === 'option' ? optionLayers(shape.inner) + 1 : 0;
}

function toPrimitive(value: any, valueType: string): any {
    if (value instanceof DurationValue || value instanceof TimeValue) {
        return value.toString();
    }
    if (value && typeof value === 'object' && typeof value.valueOf === 'function') {
        return coerceToPrimitiveType(value.valueOf(), valueType);
    }
    return coerceToPrimitiveType(value, valueType);
}

function coerceToPrimitiveType(value: any, valueType: string): any {
    if (value === undefined || value === null) {
        return value;
    }

    const bigintTypes = ['Int8', 'Int16', 'Uint8', 'Uint16'];
    if (bigintTypes.includes(valueType)) {
        if (typeof value === 'bigint') {
            return value;
        }
        if (typeof value === 'number') {
            return BigInt(Math.trunc(value));
        }
        if (typeof value === 'string') {
            return BigInt(value);
        }
    }

    return value;
}
