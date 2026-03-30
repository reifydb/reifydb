// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue,
    Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    DurationValue, TimeValue,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    NoneValue, Utf8Value,
    Uuid4Value, Uuid7Value, IdentityIdValue,
    BaseType
} from '../value';
import {ShapeNode} from '.';
import {PrimitiveToValue} from './inference';

function createValueInstance<T extends BaseType>(type: T, value: any): PrimitiveToValue<T> {
    switch (type) {
        case 'Blob':
            return new BlobValue(value) as PrimitiveToValue<T>;
        case 'Boolean':
            return new BooleanValue(value) as PrimitiveToValue<T>;
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
        case 'Duration':
            return new DurationValue(value) as PrimitiveToValue<T>;
        case 'Uuid4':
            return new Uuid4Value(value) as PrimitiveToValue<T>;
        case 'Uuid7':
            return new Uuid7Value(value) as PrimitiveToValue<T>;
        case 'Decimal':
            return new DecimalValue(value) as PrimitiveToValue<T>;
        case 'IdentityId':
            return new IdentityIdValue(value) as PrimitiveToValue<T>;
        case 'None':
            return new NoneValue() as PrimitiveToValue<T>;
        default:
            throw new Error(`Unknown type: ${type}`);
    }
}

export function parseValue(shape: ShapeNode, value: any): any {
    if (shape.kind === 'primitive') {
        if (value === null || value === undefined) {
            return undefined;
        }
        return createValueInstance(shape.type as BaseType, value);
    }

    if (shape.kind === 'object') {
        if (value === null || value === undefined) {
            return undefined;
        }
        const result: Record<string, any> = {};
        for (const [key, propShape] of Object.entries(shape.properties)) {
            result[key] = parseValue(propShape, value[key]);
        }
        return result;
    }

    if (shape.kind === 'array') {
        if (!Array.isArray(value)) {
            return [];
        }
        return value.map(item => parseValue(shape.items, item));
    }

    if (shape.kind === 'optional') {
        if (value === undefined) {
            return undefined;
        }
        return parseValue(shape.shape, value);
    }

    if (shape.kind === 'value') {
        if (value === null || value === undefined) {
            return undefined;
        }
        return createValueInstance(shape.type as BaseType, value);
    }

    throw new Error(`Unknown shape kind: ${(shape as any).kind}`);
}
