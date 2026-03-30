// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {Type} from '../value';
import {ShapeNode} from '.';

export function validateShape(shape: ShapeNode, value: any): boolean {
    if (shape.kind === 'primitive') {
        const shapeType = shape.type as Type;
        if (value === null || value === undefined) {
            return shapeType === 'None';
        }

        switch (shapeType) {
            case 'Boolean':
                return typeof value === 'boolean';
            case 'Float4':
            case 'Float8':
            case 'Int1':
            case 'Int2':
            case 'Int4':
                return typeof value === 'number';
            case 'Int8':
            case 'Int16':
            case 'Uint8':
            case 'Uint16':
                return typeof value === 'bigint' || typeof value === 'number';
            case 'Uint1':
            case 'Uint2':
            case 'Uint4':
                return typeof value === 'number' && value >= 0;
            case 'Utf8':
            case 'Time':
            case 'Duration':
            case 'Uuid4':
            case 'Uuid7':
                return typeof value === 'string';
            case 'Date':
            case 'DateTime':
                return value instanceof Date || typeof value === 'string';
            case 'Blob':
                return value instanceof Uint8Array || value instanceof ArrayBuffer;
            case 'None':
                return value === undefined;
            default:
                return false;
        }
    }

    if (shape.kind === 'object') {
        if (typeof value !== 'object' || value === null) {
            return false;
        }
        for (const [key, propShape] of Object.entries(shape.properties)) {
            if (!validateShape(propShape, value[key])) {
                return false;
            }
        }
        return true;
    }

    if (shape.kind === 'array') {
        if (!Array.isArray(value)) {
            return false;
        }
        return value.every(item => validateShape(shape.items, item));
    }

    if (shape.kind === 'optional') {
        if (value === undefined) {
            return true;
        }
        return validateShape(shape.shape, value);
    }

    if (shape.kind === 'value') {
        if (value === null || value === undefined) {
            return shape.type === 'None';
        }
        if (typeof value === 'object' && value !== null && 'type' in value && 'encode' in value) {
            return value.type === shape.type;
        }
        return false;
    }

    return false;
}
