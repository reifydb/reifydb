/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue, Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, DurationValue,
    TimeValue, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    Uint16Value, UndefinedValue, Utf8Value, Uuid4Value, Uuid7Value, IdentityIdValue,
    Value, TypeValuePair
} from './value';


export function decode(pair: TypeValuePair): Value {
    switch (pair.type) {
        case "Blob":
            return BlobValue.parse(pair.value);
        case "Boolean":
            return BooleanValue.parse(pair.value);
        case "Date":
            return DateValue.parse(pair.value);
        case "DateTime":
            return DateTimeValue.parse(pair.value);
        case "Decimal":
            return DecimalValue.parse(pair.value);
        case "Float4":
            return Float4Value.parse(pair.value);
        case "Float8":
            return Float8Value.parse(pair.value);
        case "Int1":
            return Int1Value.parse(pair.value);
        case "Int2":
            return Int2Value.parse(pair.value);
        case "Int4":
            return Int4Value.parse(pair.value);
        case "Int8":
            return Int8Value.parse(pair.value);
        case "Int16":
            return Int16Value.parse(pair.value);
        case "Duration":
            return DurationValue.parse(pair.value);
        case "Time":
            return TimeValue.parse(pair.value);
        case "Uint1":
            return Uint1Value.parse(pair.value);
        case "Uint2":
            return Uint2Value.parse(pair.value);
        case "Uint4":
            return Uint4Value.parse(pair.value);
        case "Uint8":
            return Uint8Value.parse(pair.value);
        case "Uint16":
            return Uint16Value.parse(pair.value);
        case "Undefined":
            return UndefinedValue.parse(pair.value);
        case "Utf8":
            return Utf8Value.parse(pair.value);
        case "Uuid4":
            return Uuid4Value.parse(pair.value);
        case "Uuid7":
            return Uuid7Value.parse(pair.value);
        case "IdentityId":
            return IdentityIdValue.parse(pair.value);
        default:
            throw new Error(`Unsupported type: ${pair.type}`);
    }
}