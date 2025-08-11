/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {WsClient, WsClientOptions} from "./ws";

export class Client {
    /**
     * Connect to ReifyDB via WebSocket
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected WebSocket client
     */
    static async connect_ws(url: string, options: Omit<WsClientOptions, 'url'> = {}): Promise<WsClient> {
        return WsClient.connect({url, ...options});
    }
}

export {ReifyError, Diagnostic, Span, DiagnosticColumn} from "./types";

// Re-export core Value classes
export {
    BoolValue,
    Int1Value,
    Int2Value,
    Int4Value,
    Int8Value,
    Int16Value,
    Uint1Value,
    Uint2Value,
    Uint4Value,
    Uint8Value,
    Uint16Value,
    Float4Value,
    Float8Value,
    Utf8Value,
    DateValue,
    DateTimeValue,
    TimeValue,
    IntervalValue,
    Uuid4Value,
    Uuid7Value,
    UndefinedValue,
    BlobValue,
    RowIdValue
} from "@reifydb/core";


export {WsClient, WsClientOptions} from "./ws";
export {SchemaHelpers, DEFAULT_SCHEMA} from "./schema-helpers";

// Re-export schema types from core
export {
    SchemaPatterns,
    SchemaTransformer,
    BidirectionalSchema,
    SchemaNode,
    InferSchemaType,
    InferParamType,
    InferResultType,
    InferPrimitiveSchemaResult,
    InferPrimitiveObject,
    PrimitiveType,
    ValueType
} from "@reifydb/core";

// Extended Schema with additional functions
import { Schema as CoreSchema } from "@reifydb/core";

export const Schema = {
    // Core Schema methods
    string: CoreSchema.string,
    number: CoreSchema.number,
    boolean: CoreSchema.boolean,
    bigint: CoreSchema.bigint,
    date: CoreSchema.date,
    undefined: CoreSchema.undefined,
    null: CoreSchema.null,
    boolValue: CoreSchema.boolValue,
    int1Value: CoreSchema.int1Value,
    int2Value: CoreSchema.int2Value,
    int4Value: CoreSchema.int4Value,
    int8Value: CoreSchema.int8Value,
    int16Value: CoreSchema.int16Value,
    uint1Value: CoreSchema.uint1Value,
    uint2Value: CoreSchema.uint2Value,
    uint4Value: CoreSchema.uint4Value,
    uint8Value: CoreSchema.uint8Value,
    uint16Value: CoreSchema.uint16Value,
    float4Value: CoreSchema.float4Value,
    float8Value: CoreSchema.float8Value,
    utf8Value: CoreSchema.utf8Value,
    dateValue: CoreSchema.dateValue,
    dateTimeValue: CoreSchema.dateTimeValue,
    timeValue: CoreSchema.timeValue,
    intervalValue: CoreSchema.intervalValue,
    uuid4Value: CoreSchema.uuid4Value,
    uuid7Value: CoreSchema.uuid7Value,
    undefinedValue: CoreSchema.undefinedValue,
    blobValue: CoreSchema.blobValue,
    rowIdValue: CoreSchema.rowIdValue,
    object: CoreSchema.object,
    array: CoreSchema.array,
    tuple: CoreSchema.tuple,
    union: CoreSchema.union,
    optional: CoreSchema.optional,
    auto: CoreSchema.auto,
    bidirectional: CoreSchema.bidirectional,
    
    // New convenience methods
    withPrimitiveResult: CoreSchema.withPrimitiveResult,
    primitive: CoreSchema.primitive,
    result: CoreSchema.result,
    legacyParams: CoreSchema.legacyParams
};