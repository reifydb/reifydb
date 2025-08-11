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
    Schema,
    SchemaPatterns,
    SchemaTransformer,
    BidirectionalSchema,
    SchemaNode,
    InferSchemaType,
    InferParamType,
    InferResultType,
    PrimitiveType,
    ValueType
} from "@reifydb/core";