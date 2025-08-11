/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import { WsClient, WsClientOptions } from "./ws";
export declare class Client {
    /**
     * Connect to ReifyDB via WebSocket
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected WebSocket client
     */
    static connect_ws(url: string, options?: Omit<WsClientOptions, 'url'>): Promise<WsClient>;
}
export { ReifyError, Diagnostic, Span, DiagnosticColumn } from "./types";
export { BoolValue, Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value, Float4Value, Float8Value, Utf8Value, DateValue, DateTimeValue, TimeValue, IntervalValue, Uuid4Value, Uuid7Value, UndefinedValue, BlobValue, RowIdValue } from "@reifydb/core";
export { WsClient, WsClientOptions } from "./ws";
export { SchemaHelpers, DEFAULT_SCHEMA } from "./schema-helpers";
export { SchemaPatterns, SchemaTransformer, BidirectionalSchema, SchemaNode, InferSchemaType, InferParamType, InferResultType, InferPrimitiveSchemaResult, InferPrimitiveObject, PrimitiveType, ValueType } from "@reifydb/core";
import { Schema as CoreSchema } from "@reifydb/core";
export declare const Schema: {
    string: typeof CoreSchema.string;
    number: typeof CoreSchema.number;
    boolean: typeof CoreSchema.boolean;
    bigint: typeof CoreSchema.bigint;
    date: typeof CoreSchema.date;
    undefined: typeof CoreSchema.undefined;
    null: typeof CoreSchema.null;
    boolValue: typeof CoreSchema.boolValue;
    int1Value: typeof CoreSchema.int1Value;
    int2Value: typeof CoreSchema.int2Value;
    int4Value: typeof CoreSchema.int4Value;
    int8Value: typeof CoreSchema.int8Value;
    int16Value: typeof CoreSchema.int16Value;
    uint1Value: typeof CoreSchema.uint1Value;
    uint2Value: typeof CoreSchema.uint2Value;
    uint4Value: typeof CoreSchema.uint4Value;
    uint8Value: typeof CoreSchema.uint8Value;
    uint16Value: typeof CoreSchema.uint16Value;
    float4Value: typeof CoreSchema.float4Value;
    float8Value: typeof CoreSchema.float8Value;
    utf8Value: typeof CoreSchema.utf8Value;
    dateValue: typeof CoreSchema.dateValue;
    dateTimeValue: typeof CoreSchema.dateTimeValue;
    timeValue: typeof CoreSchema.timeValue;
    intervalValue: typeof CoreSchema.intervalValue;
    uuid4Value: typeof CoreSchema.uuid4Value;
    uuid7Value: typeof CoreSchema.uuid7Value;
    undefinedValue: typeof CoreSchema.undefinedValue;
    blobValue: typeof CoreSchema.blobValue;
    rowIdValue: typeof CoreSchema.rowIdValue;
    object: typeof CoreSchema.object;
    array: typeof CoreSchema.array;
    tuple: typeof CoreSchema.tuple;
    union: typeof CoreSchema.union;
    optional: typeof CoreSchema.optional;
    auto: typeof CoreSchema.auto;
    bidirectional: typeof CoreSchema.bidirectional;
    withPrimitiveResult: typeof CoreSchema.withPrimitiveResult;
    primitive: typeof CoreSchema.primitive;
    result: typeof CoreSchema.result;
    legacyParams: typeof CoreSchema.legacyParams;
};
