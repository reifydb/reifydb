/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import {decode, Value, TypeValuePair, BidirectionalSchema, SchemaTransformer} from "@reifydb/core";

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
     * Execute a command with schema-based parameter encoding and result decoding
     * For single statement commands, TResult represents the result of that statement
     * For multi-statement commands, TResult should be a tuple type representing all results
     */
    async command<TResult = any>(
        statement: string,
        params: any,
        schema: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]>;

    async command<TResult = any>(
        statement: string,
        schema: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]>;

    async command<TResult = any>(
        statement: string,
        paramsOrSchema: any | BidirectionalSchema,
        schema?: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]> {
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
        
        // Encode params using schema
        let encodedParams: any = undefined;
        if (actualSchema && actualSchema.params && actualParams !== undefined) {
            encodedParams = this.encodeWithSchema(actualParams, actualSchema.params);
        }
        
        const result = await this.send({
            id,
            type: "Command",
            payload: {
                statements: [statement],
                params: encodedParams
            },
        });
        
        // Decode results if schema provided
        if (actualSchema && actualSchema.result) {
            return result.map((frame: any) => 
                frame.map((row: any) => SchemaTransformer.decodeResult(row, actualSchema.result!))
            ) as any;
        }
        
        return result as any;
    }

    /**
     * Execute a query with schema-based parameter encoding and result decoding
     * For single statement queries, TResult represents the result of that statement
     * For multi-statement queries, TResult should be a tuple type representing all results
     */
    async query<TResult = any>(
        statement: string,
        params: any,
        schema: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]>;

    async query<TResult = any>(
        statement: string,
        schema: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]>;

    async query<TResult = any>(
        statement: string,
        paramsOrSchema: any | BidirectionalSchema,
        schema?: BidirectionalSchema
    ): Promise<TResult extends readonly any[] ? TResult : TResult[]> {
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
        
        // Encode params using schema
        let encodedParams: any = undefined;
        if (actualSchema && actualSchema.params && actualParams !== undefined) {
            encodedParams = this.encodeWithSchema(actualParams, actualSchema.params);
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
        if (actualSchema && actualSchema.result) {
            return result.map((frame: any) => 
                frame.map((row: any) => SchemaTransformer.decodeResult(row, actualSchema.result!))
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