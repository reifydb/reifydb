import { BidirectionalSchema, InferPrimitiveSchemaResult, InferPrimitiveObject, ObjectSchemaNode } from "@reifydb/core";
import { CommandRequest, QueryRequest } from "./types";
export interface WsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
}
export declare class WsClient {
    private options;
    private nextId;
    private socket;
    private pending;
    private constructor();
    static connect(options: WsClientOptions): Promise<WsClient>;
    /**
     * Execute a command with per-frame schema for result type inference
     */
    command<S1 extends ObjectSchemaNode>(statement: string, params: any, schemas: readonly [S1]): Promise<[InferPrimitiveObject<S1>[]]>;
    /**
     * Execute a query with schema-based parameter encoding and result decoding
     * With automatic type inference for primitive schemas
     */
    query<S extends {
        __primitiveFields: Record<string, any>;
    } | {
        __constructorFields: Record<string, any>;
    } | {
        __typeFields: Record<string, any>;
    } | {
        __resultFields: Record<string, any>;
    }>(statement: string, params: any, schema: S): Promise<InferPrimitiveSchemaResult<S>[]>;
    query<S extends {
        __primitiveFields: Record<string, any>;
    } | {
        __constructorFields: Record<string, any>;
    } | {
        __typeFields: Record<string, any>;
    } | {
        __resultFields: Record<string, any>;
    }>(statement: string, schema: S): Promise<InferPrimitiveSchemaResult<S>[]>;
    query<TResult = any>(statement: string, params: any, schema: any): Promise<TResult[]>;
    query<TResult = any>(statement: string, schema: any): Promise<TResult[]>;
    query<TResult = any>(statement: string, params: any, schema: BidirectionalSchema): Promise<TResult[]>;
    query<TResult = any>(statement: string, schema: BidirectionalSchema): Promise<TResult[]>;
    send(req: CommandRequest | QueryRequest): Promise<any>;
    private encodeWithSchema;
    private isValueObjectParams;
    private encodeValueObjectParams;
    private isLegacySchema;
    private encodePrimitiveParams;
    private transformResult;
    private fallbackEncode;
    disconnect(): void;
}
