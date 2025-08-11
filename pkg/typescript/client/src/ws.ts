/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import {decode, Value, TypeValuePair, BidirectionalSchema, SchemaTransformer, InferPrimitiveSchemaResult, SchemaNode, InferPrimitiveObject, ObjectSchemaNode} from "@reifydb/core";

import {
    CommandRequest,
    CommandResponse,
    ErrorResponse,
    QueryRequest,
    QueryResponse,
    ReifyError,
    Column,
    Params
} from "./types";

export interface WsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
}

type ResponsePayload = ErrorResponse | CommandResponse | QueryResponse;

async function createWebSocket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    } else {
        //@ts-ignore
        const wsModule = await import("ws");
        return new wsModule.WebSocket(url);
    }
}


export class WsClient {
    private options: WsClientOptions;
    private nextId: number;
    private socket: WebSocket;
    private pending = new Map<string, (response: ResponsePayload) => void>();

    private constructor(socket: WebSocket, options: WsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            const {id, type, payload} = msg;

            const handler = this.pending.get(id);
            if (!handler) {
                return;
            }

            this.pending.delete(id);
            handler({id, type, payload});
        };

        this.socket.onerror = (err) => {
            console.error("WebSocket error", err);
        };
    }

    static async connect(options: WsClientOptions): Promise<WsClient> {
        const socket = await createWebSocket(options.url);

        // Wait for connection to open if not already open
        if (socket.readyState !== socket.OPEN) {
            await new Promise<void>((resolve, reject) => {
                const onOpen = () => {
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    resolve();
                };

                const onError = () => {
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    reject(new Error("WebSocket connection failed"));
                };

                socket.addEventListener("open", onOpen);
                socket.addEventListener("error", onError);
            });
        }

        socket.send("{\"id\":\"reifydb-auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

        return new WsClient(socket, options);
    }

    /**
     * Execute a command with per-frame schema for result type inference
     */
    async command<const T extends readonly [ObjectSchemaNode]>(
        statement: string,
        params: any,
        schemas: T
    ): Promise<[InferPrimitiveObject<T[0]>[]]>;

    async command(
        statement: string,
        params: any,
        schemas: readonly SchemaNode[]
    ): Promise<any> {
        const id = `req-${this.nextId++}`;
        
        // Encode params using fallback encoding
        const encodedParams = this.encodeWithSchema(params, null);
        
        const result = await this.send({
            id,
            type: "Command",
            payload: {
                statements: [statement],
                params: encodedParams
            },
        });
        
        // Transform each frame with its corresponding schema
        const transformedFrames = result.map((frame: any, frameIndex: number) => {
            const frameSchema = schemas[frameIndex];
            if (!frameSchema) {
                return frame; // No schema for this frame, return as-is
            }
            return frame.map((row: any) => this.transformResult(row, frameSchema));
        });
        
        return transformedFrames as any;
    }

    /**
     * Execute a query with schema-based parameter encoding and result decoding
     * With automatic type inference for primitive schemas
     */
    async query<S extends { __primitiveFields: Record<string, any> } | { __constructorFields: Record<string, any> } | { __typeFields: Record<string, any> } | { __resultFields: Record<string, any> }>(
        statement: string,
        params: any,
        schema: S
    ): Promise<InferPrimitiveSchemaResult<S>[]>;

    async query<S extends { __primitiveFields: Record<string, any> } | { __constructorFields: Record<string, any> } | { __typeFields: Record<string, any> } | { __resultFields: Record<string, any> }>(
        statement: string,
        schema: S
    ): Promise<InferPrimitiveSchemaResult<S>[]>;

    // Accept any schema node for backwards compatibility
    async query<TResult = any>(
        statement: string,
        params: any,
        schema: any
    ): Promise<TResult[]>;

    async query<TResult = any>(
        statement: string,
        schema: any
    ): Promise<TResult[]>;

    async query<TResult = any>(
        statement: string,
        params: any,
        schema: BidirectionalSchema
    ): Promise<TResult[]>;

    async query<TResult = any>(
        statement: string,
        schema: BidirectionalSchema
    ): Promise<TResult[]>;

    async query<TResult = any>(
        statement: string,
        paramsOrSchema: any | BidirectionalSchema,
        schema?: BidirectionalSchema
    ): Promise<TResult[]> {
        const id = `req-${this.nextId++}`;
        
        let actualParams: any = undefined;
        let actualSchema: BidirectionalSchema;
        
        // Handle overloads: (statement, params, schema) or (statement, schema)
        if (schema) {
            actualParams = paramsOrSchema;
            actualSchema = schema;
        } else {
            actualSchema = paramsOrSchema;
        }
        
        // Handle raw schema nodes (not BidirectionalSchema)
        let resultSchema: any = null;
        if (actualSchema) {
            if ('result' in actualSchema) {
                // BidirectionalSchema
                resultSchema = actualSchema.result;
            } else if ('kind' in actualSchema) {
                // Raw schema node
                resultSchema = actualSchema;
            }
        }
        
        // Encode params using schema (for now, just use fallback encoding)
        let encodedParams: any = undefined;
        if (actualParams !== undefined) {
            if (actualSchema && actualSchema.params) {
                encodedParams = this.encodeWithSchema(actualParams, actualSchema.params);
            } else {
                // Fallback encoding for raw schema nodes
                encodedParams = this.encodeWithSchema(actualParams, null);
            }
        }
        
        const result = await this.send({
            id,
            type: "Query",
            payload: {
                statements: [statement],
                params: encodedParams
            },
        });
        
        // Decode results if schema provided
        if (resultSchema) {
            return result.map((frame: any) => 
                frame.map((row: any) => this.transformResult(row, resultSchema))
            ) as any;
        }
        
        return result as any;
    }

    async send(req: CommandRequest | QueryRequest): Promise<any> {
        const id = req.id;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, this.options.timeoutMs);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify(req));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);

        }

        if (response.type !== req.type) {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        return response.payload.frames.map((frame) =>
            columnsToRows(frame.columns)
        );
    }

    private encodeWithSchema(params: any, schema: any): Params {        
        // For LEGACY_SCHEMA-like usage, if we have Value objects, encode them directly
        if (this.isValueObjectParams(params)) {
            return this.encodeValueObjectParams(params);
        }
        
        // If no schema provided, use fallback encoding
        if (!schema) {
            return this.encodePrimitiveParams(params);
        }
        
        // For primitive parameters with LEGACY_SCHEMA, encode directly using fallback
        if (this.isLegacySchema(schema)) {
            return this.encodePrimitiveParams(params);
        }
        
        const encodedParams = SchemaTransformer.encodeParams(params, schema);
        
        // Convert the schema-encoded result to the expected Params format
        if (Array.isArray(encodedParams)) {
            return encodedParams.map(param => {
                if (param && typeof param === 'object' && 'encode' in param) {
                    return param.encode();
                }
                // Fallback encoding for primitives
                return this.fallbackEncode(param);
            });
        } else {
            const encoded: Record<string, TypeValuePair> = {};
            for (const [key, value] of Object.entries(encodedParams)) {
                if (value && typeof value === 'object' && 'encode' in value) {
                    encoded[key] = (value as Value).encode();
                } else {
                    encoded[key] = this.fallbackEncode(value);
                }
            }
            return encoded;
        }
    }

    private isValueObjectParams(params: any): boolean {
        if (!params || typeof params !== 'object') {
            return false;
        }
        
        if (Array.isArray(params)) {
            return params.some(p => p && typeof p === 'object' && 'encode' in p);
        }
        
        return Object.values(params).some(v => v && typeof v === 'object' && 'encode' in v);
    }

    private encodeValueObjectParams(params: any): Params {
        if (Array.isArray(params)) {
            return params.map(param => {
                if (param && typeof param === 'object' && 'encode' in param) {
                    return param.encode();
                }
                return this.fallbackEncode(param);
            });
        } else {
            const encoded: Record<string, TypeValuePair> = {};
            for (const [key, value] of Object.entries(params)) {
                if (value && typeof value === 'object' && 'encode' in value) {
                    encoded[key] = (value as Value).encode();
                } else {
                    encoded[key] = this.fallbackEncode(value);
                }
            }
            return encoded;
        }
    }

    private isLegacySchema(schema: any): boolean {
        // Check if this is the LEGACY_SCHEMA by looking for its characteristic structure
        // LEGACY_SCHEMA has params: Schema.optional(Schema.union(...)) and no result schema
        return schema && 
               schema.kind === 'optional' && 
               schema.schema && 
               schema.schema.kind === 'union';
    }

    private encodePrimitiveParams(params: any): Params {
        if (Array.isArray(params)) {
            return params.map(param => this.fallbackEncode(param));
        } else {
            const encoded: Record<string, TypeValuePair> = {};
            for (const [key, value] of Object.entries(params)) {
                encoded[key] = this.fallbackEncode(value);
            }
            return encoded;
        }
    }

    private transformResult(row: any, resultSchema: any): any {
        console.log('transformResult called with:', { row, resultSchema });
        
        // Handle primitive schema transformation
        if (resultSchema && resultSchema.kind === 'primitive') {
            console.log('Using primitive branch');
            const transformedRow: any = {};
            for (const [key, value] of Object.entries(row)) {
                // If it's a Value object with .value property, extract the primitive
                if (value && typeof value === 'object' && 'value' in value) {
                    transformedRow[key] = (value as any).value;
                } else {
                    transformedRow[key] = value;
                }
            }
            console.log('Transformed row:', transformedRow);
            return transformedRow;
        }
        
        // Handle union schema - check if first type is an object with primitive properties
        if (resultSchema && resultSchema.kind === 'union' && resultSchema.types && resultSchema.types.length > 0) {
            const firstType = resultSchema.types[0];
            if (firstType && firstType.kind === 'object' && firstType.properties) {
                console.log('Using union schema with object primitive conversion');
                const transformedRow: any = {};
                for (const [key, value] of Object.entries(row)) {
                    const propertySchema = firstType.properties[key];
                    if (propertySchema && propertySchema.kind === 'primitive') {
                        // Convert Value objects to primitives for primitive schema properties
                        if (value && typeof value === 'object' && 'value' in value) {
                            transformedRow[key] = (value as any).value;
                        } else {
                            transformedRow[key] = value;
                        }
                    } else {
                        // Keep as-is for non-primitive properties
                        transformedRow[key] = value;
                    }
                }
                console.log('Transformed row:', transformedRow);
                return transformedRow;
            }
        }
        
        // Handle object schema with primitive properties - extract primitives from Value objects
        if (resultSchema && resultSchema.kind === 'object' && resultSchema.properties) {
            console.log('Using object schema with primitive conversion');
            const transformedRow: any = {};
            for (const [key, value] of Object.entries(row)) {
                const propertySchema = resultSchema.properties[key];
                if (propertySchema && propertySchema.kind === 'primitive') {
                    // Convert Value objects to primitives for primitive schema properties
                    if (value && typeof value === 'object' && 'value' in value) {
                        transformedRow[key] = (value as any).value;
                    } else {
                        transformedRow[key] = value;
                    }
                } else {
                    // Keep as-is for non-primitive properties
                    transformedRow[key] = value;
                }
            }
            console.log('Transformed row:', transformedRow);
            return transformedRow;
        }
        
        // Default to using SchemaTransformer
        console.log('Using SchemaTransformer.decodeResult');
        const result = SchemaTransformer.decodeResult(row, resultSchema);
        console.log('SchemaTransformer.decodeResult returned:', result);
        return result;
    }

    private fallbackEncode(value: any): TypeValuePair {
        if (value === null || value === undefined) {
            return { type: 'Undefined', value: '⟪undefined⟫' };
        }
        
        switch (typeof value) {
            case 'boolean':
                return { type: 'Bool', value: value.toString() };
            case 'number':
                if (Number.isInteger(value)) {
                    if (value >= -128 && value <= 127) {
                        return { type: 'Int1', value: value.toString() };
                    } else if (value >= -32768 && value <= 32767) {
                        return { type: 'Int2', value: value.toString() };
                    } else if (value >= -2147483648 && value <= 2147483647) {
                        return { type: 'Int4', value: value.toString() };
                    } else {
                        return { type: 'Int8', value: value.toString() };
                    }
                } else {
                    return { type: 'Float8', value: value.toString() };
                }
            case 'string':
                return { type: 'Utf8', value: value };
            case 'bigint':
                return { type: 'Int8', value: value.toString() };
            default:
                if (value instanceof Date) {
                    return { type: 'DateTime', value: value.toISOString() };
                }
                throw new Error(`Unsupported parameter type: ${typeof value}`);
        }
    }

    disconnect() {
        this.socket.close();
    }
}


function columnsToRows(columns: Column[]): Record<string, Value>[] {
    const rowCount = columns[0]?.data.length ?? 0;
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.ty, value: col.data[i]});
        }
        return row;
    });
}