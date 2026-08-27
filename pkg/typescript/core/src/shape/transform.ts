// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {ShapeNode} from '.';
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
                if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                    const rawValue = (value as any).valueOf();
                    transformedRow[outputKey] = coerceToPrimitiveType(rawValue, propertyShape.type);
                } else {
                    transformedRow[outputKey] = coerceToPrimitiveType(value, propertyShape.type);
                }
            } else if (propertyShape && propertyShape.kind === 'value') {
                transformedRow[outputKey] = value;
            } else {
                transformedRow[outputKey] = propertyShape ? transformResult(value, propertyShape) : value;
            }
        }
        return transformedRow;
    }

    if (resultShape && resultShape.kind === 'primitive') {
        if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
            return coerceToPrimitiveType(row.valueOf(), resultShape.type);
        }
        return coerceToPrimitiveType(row, resultShape.type);
    }

    if (resultShape && resultShape.kind === 'value') {
        return row;
    }

    if (resultShape && resultShape.kind === 'array') {
        if (Array.isArray(row)) {
            return row.map((item: any) => transformResult(item, resultShape.items));
        }
        return row;
    }

    if (resultShape && resultShape.kind === 'optional') {
        if (row === undefined) {
            return undefined;
        }
        if (row === null) {
            return null;
        }
        return transformResult(row, resultShape.shape);
    }

    return row;
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
