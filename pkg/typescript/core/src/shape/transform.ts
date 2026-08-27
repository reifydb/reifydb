// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {ShapeNode} from '.';
import {FrameResults} from '../types';

export function transform_frames<const S extends readonly ShapeNode[]>(
    frames: any[][],
    shapes: S
): FrameResults<S> {
    const transformed: any[][] = frames.map((frame: any[], frame_index: number) => {
        const frame_shape = shapes[frame_index];
        if (!frame_shape) {
            return frame;
        }
        return frame.map((row: any) => transform_result(row, frame_shape));
    });
    return transformed as FrameResults<S>;
}

export function transform_result(row: any, result_shape: any): any {
    if (result_shape && result_shape.kind === 'object' && result_shape.properties) {
        const transformed_row: any = {};
        for (const [key, value] of Object.entries(row)) {
            const property_shape = result_shape.properties[key];
            if (property_shape && property_shape.kind === 'primitive') {
                if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                    const raw_value = (value as any).valueOf();
                    transformed_row[key] = coerce_to_primitive_type(raw_value, property_shape.type);
                } else {
                    transformed_row[key] = coerce_to_primitive_type(value, property_shape.type);
                }
            } else if (property_shape && property_shape.kind === 'value') {
                transformed_row[key] = value;
            } else {
                transformed_row[key] = property_shape ? transform_result(value, property_shape) : value;
            }
        }
        return transformed_row;
    }

    if (result_shape && result_shape.kind === 'primitive') {
        if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
            return coerce_to_primitive_type(row.valueOf(), result_shape.type);
        }
        return coerce_to_primitive_type(row, result_shape.type);
    }

    if (result_shape && result_shape.kind === 'value') {
        return row;
    }

    if (result_shape && result_shape.kind === 'array') {
        if (Array.isArray(row)) {
            return row.map((item: any) => transform_result(item, result_shape.items));
        }
        return row;
    }

    if (result_shape && result_shape.kind === 'optional') {
        if (row === undefined || row === null) {
            return undefined;
        }
        return transform_result(row, result_shape.shape);
    }

    return row;
}

function coerce_to_primitive_type(value: any, value_type: string): any {
    if (value === undefined || value === null) {
        return value;
    }

    const bigint_types = ['Int8', 'Int16', 'Uint8', 'Uint16'];
    if (bigint_types.includes(value_type)) {
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
