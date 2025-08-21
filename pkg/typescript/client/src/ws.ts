/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import {
    decode,
    Value
} from "@reifydb/core";
import type {
    SchemaNode,
    InferSchemas,
    FrameResults
} from "@reifydb/core";

import type {
    CommandRequest,
    CommandResponse,
    QueryRequest,
    QueryResponse,
    Column,
    Params,
    ErrorResponse
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";

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
     * Execute command(s) with schemas for each statement for proper type inference
     * @param statements - Single statement or array of RQL commands
     * @param params - Parameters for the commands (use null or {} if no params)
     * @param schemas - Schema for each statement's result
     */
    async command<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<FrameResults<S>> {
        const id = `req-${this.nextId++}`;

        // Normalize statements to array
        const statementArray = Array.isArray(statements) ? statements : [statements];

        // Encode params without schema assumptions
        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const result = await this.send({
            id,
            type: "Command",
            payload: {
                statements: statementArray,
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

        return transformedFrames as FrameResults<S>;
    }


    /**
     * Execute query(s) with schemas for each statement for proper type inference
     * @param statements - Single statement or array of RQL queries
     * @param params - Parameters for the queries (use null or {} if no params)
     * @param schemas - Schema for each statement's result
     */
    async query<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<FrameResults<S>> {
        const id = `req-${this.nextId++}`;

        // Normalize statements to array
        const statementArray = Array.isArray(statements) ? statements : [statements];

        // Encode params without schema assumptions
        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const result = await this.send({
            id,
            type: "Query",
            payload: {
                statements: statementArray,
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

        return transformedFrames as FrameResults<S>;
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


    private transformResult(row: any, resultSchema: any): any {
        // Handle object schema with primitive or value properties
        if (resultSchema && resultSchema.kind === 'object' && resultSchema.properties) {
            const transformedRow: any = {};
            for (const [key, value] of Object.entries(row)) {
                const propertySchema = resultSchema.properties[key];
                if (propertySchema && propertySchema.kind === 'primitive') {
                    // Convert Value objects to primitives for primitive schema properties
                    if (value && typeof value === 'object' && 'value' in value) {
                        const rawValue = (value as any).value;
                        // Special handling for RowNumber - ensure it's returned as bigint
                        if (propertySchema.type === 'RowNumber' && typeof rawValue === 'number') {
                            transformedRow[key] = BigInt(rawValue);
                        } else {
                            transformedRow[key] = rawValue;
                        }
                    } else {
                        transformedRow[key] = value;
                    }
                } else if (propertySchema && propertySchema.kind === 'value') {
                    // Keep Value objects as-is for value schema properties
                    transformedRow[key] = value;
                } else {
                    // Recursively transform nested structures
                    transformedRow[key] = propertySchema ? this.transformResult(value, propertySchema) : value;
                }
            }
            return transformedRow;
        }

        // Handle primitive schema transformation
        if (resultSchema && resultSchema.kind === 'primitive') {
            // Single primitive value - extract from Value object if needed
            if (row && typeof row === 'object' && 'value' in row) {
                const rawValue = row.value;
                // Special handling for RowNumber - ensure it's returned as bigint
                if (resultSchema.type === 'RowNumber' && typeof rawValue === 'number') {
                    return BigInt(rawValue);
                }
                return rawValue;
            }
            return row;
        }

        // Handle value schema transformation - keep Value objects as-is
        if (resultSchema && resultSchema.kind === 'value') {
            return row;
        }

        // Handle array schema
        if (resultSchema && resultSchema.kind === 'array') {
            if (Array.isArray(row)) {
                return row.map((item: any) => this.transformResult(item, resultSchema.items));
            }
            return row;
        }

        // Handle optional schema
        if (resultSchema && resultSchema.kind === 'optional') {
            if (row === undefined || row === null) {
                return undefined;
            }
            return this.transformResult(row, resultSchema.schema);
        }

        // Default: return as-is
        return row;
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