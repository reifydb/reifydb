/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

export type Type =
    | "Blob"
    | "Bool"
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Utf8"
    | "Date" | "DateTime" | "Time" | "Interval"
    | "Uuid4" | "Uuid7"
    | "Undefined"
    | "RowId";

export interface TypeValuePair {
    type: Type;
    value: string;
}

export abstract class Value {
    abstract readonly type: Type;

    abstract encode(): TypeValuePair;
}