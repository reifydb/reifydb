/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {
    BlobValue, BoolValue, DateValue, DateTimeValue,
    Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    IntervalValue, TimeValue,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    RowNumberValue, UndefinedValue, Utf8Value,
    Uuid4Value, Uuid7Value,
    Type
} from '../value';
import {SchemaNode} from '.';
import {PrimitiveToValue} from './inference';

function createValueInstance<T extends Type>(type: T, value: any): PrimitiveToValue<T> {
    switch (type) {
        case 'Blob':
            return new BlobValue(value) as PrimitiveToValue<T>;
        case 'Bool':
            return new BoolValue(value) as PrimitiveToValue<T>;
        case 'Float4':
            return new Float4Value(value) as PrimitiveToValue<T>;
        case 'Float8':
            return new Float8Value(value) as PrimitiveToValue<T>;
        case 'Int1':
            return new Int1Value(value) as PrimitiveToValue<T>;
        case 'Int2':
            return new Int2Value(value) as PrimitiveToValue<T>;
        case 'Int4':
            return new Int4Value(value) as PrimitiveToValue<T>;
        case 'Int8':
            return new Int8Value(value) as PrimitiveToValue<T>;
        case 'Int16':
            return new Int16Value(value) as PrimitiveToValue<T>;
        case 'Uint1':
            return new Uint1Value(value) as PrimitiveToValue<T>;
        case 'Uint2':
            return new Uint2Value(value) as PrimitiveToValue<T>;
        case 'Uint4':
            return new Uint4Value(value) as PrimitiveToValue<T>;
        case 'Uint8':
            return new Uint8Value(value) as PrimitiveToValue<T>;
        case 'Uint16':
            return new Uint16Value(value) as PrimitiveToValue<T>;
        case 'Utf8':
            return new Utf8Value(value) as PrimitiveToValue<T>;
        case 'Date':
            return new DateValue(value) as PrimitiveToValue<T>;
        case 'DateTime':
            return new DateTimeValue(value) as PrimitiveToValue<T>;
        case 'Time':
            return new TimeValue(value) as PrimitiveToValue<T>;
        case 'Interval':
            return new IntervalValue(value) as PrimitiveToValue<T>;
        case 'Uuid4':
            return new Uuid4Value(value) as PrimitiveToValue<T>;
        case 'Uuid7':
            return new Uuid7Value(value) as PrimitiveToValue<T>;
        case 'Undefined':
            return new UndefinedValue() as PrimitiveToValue<T>;
        case 'RowNumber':
            return new RowNumberValue(value) as PrimitiveToValue<T>;
        default:
            throw new Error(`Unknown type: ${type}`);
    }
}

export function parseValue(schema: SchemaNode, value: any): any {
    if (schema.kind === 'primitive') {
        if (value === null || value === undefined) {
            return undefined;
        }
        return createValueInstance(schema.type as Type, value);
    }

    if (schema.kind === 'object') {
        if (value === null || value === undefined) {
            return undefined;
        }
        const result: Record<string, any> = {};
        for (const [key, propSchema] of Object.entries(schema.properties)) {
            result[key] = parseValue(propSchema, value[key]);
        }
        return result;
    }

    if (schema.kind === 'array') {
        if (!Array.isArray(value)) {
            return [];
        }
        return value.map(item => parseValue(schema.items, item));
    }

    if (schema.kind === 'optional') {
        if (value === undefined) {
            return undefined;
        }
        return parseValue(schema.schema, value);
    }

    throw new Error(`Unknown schema kind: ${(schema as any).kind}`);
}