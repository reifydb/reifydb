// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue, DigestValue, Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, DurationValue,
    TimeValue, Uint1Value, Uint2Value, Uint4Value, Uint8Value,
    Uint16Value, NoneValue, Utf8Value, Uuid4Value, Uuid7Value, IdentityIdValue,
    ListValue, RecordValue,
    Value, TypeValuePair, Type, isDigestType, isListType, isOptionType, isRecordType, unwrapOptionType, optionDepth, innerOfOption
} from './value';
import {noneMarkerDepth} from './constant';
import {Column} from './types';


function stripOptionLayers(type: Type, layers: number): Type {
    return layers === 0 || !isOptionType(type) ? type : stripOptionLayers(innerOfOption(type), layers - 1);
}

export function decode(pair: TypeValuePair): Value {
    if (isOptionType(pair.type)) {
        const noneAt = typeof pair.value === 'string' ? noneMarkerDepth(pair.value) : undefined;
        if (noneAt === undefined) {
            return decode({type: unwrapOptionType(pair.type), value: pair.value});
        }
        const depth = optionDepth(pair.type);
        if (noneAt >= depth) {
            throw new Error(`none under ${noneAt} Some layers cannot fit an option of depth ${depth}`);
        }
        return new NoneValue(stripOptionLayers(pair.type, noneAt + 1));
    }

    if (isDigestType(pair.type)) {
        return DigestValue.parse(pair.value as string, pair.type);
    }

    if (isListType(pair.type)) {
        if (!Array.isArray(pair.value)) {
            throw new Error(`List value must be a JSON array, got ${typeof pair.value}`);
        }
        const elementType = pair.type.List;
        return new ListValue(pair.value.map(item => decode({type: elementType, value: item})), elementType);
    }

    if (isRecordType(pair.type)) {
        if (typeof pair.value !== 'object' || pair.value === null || Array.isArray(pair.value)) {
            throw new Error(`Record value must be a JSON object, got ${typeof pair.value}`);
        }
        const obj = pair.value;
        const fields: Record<string, Value> = {};
        for (const field of pair.type.Record) {
            if (!(field.name in obj)) {
                throw new Error(`record is missing field '${field.name}'`);
            }
            fields[field.name] = decode({type: field.type, value: obj[field.name]});
        }
        return new RecordValue(fields);
    }

    if (typeof pair.value !== 'string') {
        throw new Error(`Cell for type ${pair.type} must be a JSON string, got ${typeof pair.value}`);
    }
    const value = pair.value;

    switch (pair.type) {
        case "Blob":
            return BlobValue.parse(value);
        case "Boolean":
            return BooleanValue.parse(value);
        case "Date":
            return DateValue.parse(value);
        case "DateTime":
            return DateTimeValue.parse(value);
        case "Decimal":
            return DecimalValue.parse(value);
        case "Float4":
            return Float4Value.parse(value);
        case "Float8":
            return Float8Value.parse(value);
        case "Int1":
            return Int1Value.parse(value);
        case "Int2":
            return Int2Value.parse(value);
        case "Int4":
            return Int4Value.parse(value);
        case "Int8":
            return Int8Value.parse(value);
        case "Int16":
            return Int16Value.parse(value);
        case "Duration":
            return DurationValue.parse(value);
        case "Time":
            return TimeValue.parse(value);
        case "Uint1":
            return Uint1Value.parse(value);
        case "Uint2":
            return Uint2Value.parse(value);
        case "Uint4":
            return Uint4Value.parse(value);
        case "Uint8":
            return Uint8Value.parse(value);
        case "Uint16":
            return Uint16Value.parse(value);
        case "None":
            return NoneValue.parse(value);
        case "Utf8":
            return Utf8Value.parse(value);
        case "Uuid4":
            return Uuid4Value.parse(value);
        case "Uuid7":
            return Uuid7Value.parse(value);
        case "IdentityId":
            return IdentityIdValue.parse(value);
        default:
            throw new Error(`Unsupported type: ${pair.type}`);
    }
}

export function columnsToRows(columns: Column[]): Record<string, Value>[] {
    const rowCount = columns[0]?.payload.length ?? 0;
    for (const column of columns) {
        if (column.payload.length !== rowCount) {
            throw new Error(
                `column ${column.name} carries ${column.payload.length} cells where ${columns[0].name} carries ${rowCount}`
            );
        }
    }
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.type, value: col.payload[i]});
        }
        return row;
    });
}
