/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type} from '../value';
import {SchemaNode} from '.';

export function validateSchema(schema: SchemaNode, value: any): boolean {
    if (schema.kind === 'primitive') {
        const schemaType = schema.type as Type;
        if (value === null || value === undefined) {
            return schemaType === 'Undefined';
        }

        switch (schemaType) {
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
            case 'Interval':
            case 'Uuid4':
            case 'Uuid7':
            case 'RowNumber':
                return typeof value === 'string';
            case 'Date':
            case 'DateTime':
                return value instanceof Date || typeof value === 'string';
            case 'Blob':
                return value instanceof Uint8Array || value instanceof ArrayBuffer;
            case 'Undefined':
                return value === undefined;
            default:
                return false;
        }
    }

    if (schema.kind === 'object') {
        if (typeof value !== 'object' || value === null) {
            return false;
        }
        for (const [key, propSchema] of Object.entries(schema.properties)) {
            if (!validateSchema(propSchema, value[key])) {
                return false;
            }
        }
        return true;
    }

    if (schema.kind === 'array') {
        if (!Array.isArray(value)) {
            return false;
        }
        return value.every(item => validateSchema(schema.items, item));
    }

    if (schema.kind === 'optional') {
        if (value === undefined) {
            return true;
        }
        return validateSchema(schema.schema, value);
    }

    return false;
}