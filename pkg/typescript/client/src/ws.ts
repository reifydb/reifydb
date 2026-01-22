// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {
    decode,
    Value
} from "@reifydb/core";
import type {
    SchemaNode,
    FrameResults,
} from "@reifydb/core";

import type {
    CommandRequest,
    CommandResponse,
    QueryRequest,
    QueryResponse,
    Column,
    ErrorResponse,
    SubscribeRequest,
    SubscribedResponse,
    UnsubscribeRequest,
    UnsubscribedResponse,
    ChangeMessage,
    SubscriptionCallbacks
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";

export interface WsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    maxReconnectAttempts?: number;
    reconnectDelayMs?: number;
}

interface SubscriptionState<T = any> {
    subscriptionId: string;
    query: string;
    params?: any;
    schema?: SchemaNode;
    callbacks: SubscriptionCallbacks<T>;
}

type ResponsePayload = ErrorResponse | CommandResponse | QueryResponse | SubscribedResponse | UnsubscribedResponse;

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
    private reconnectAttempts: number = 0;
    private shouldReconnect: boolean = true;
    private isReconnecting: boolean = false;
    private subscriptions = new Map<string, SubscriptionState>();

    private constructor(socket: WebSocket, options: WsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.setupSocketHandlers();
    }

    static async connect(options: WsClientOptions): Promise<WsClient> {
        const socket = await createWebSocket(options.url);

        // Wait for connection to open if not already open, with timeout
        if (socket.readyState !== 1) {
            const connectionTimeoutMs = 30000; // 30 second connection timeout
            await new Promise<void>((resolve, reject) => {
                const connectionTimeout = setTimeout(() => {
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    socket.close();
                    reject(new Error(`WebSocket connection timeout after ${connectionTimeoutMs}ms`));
                }, connectionTimeoutMs);

                const onOpen = () => {
                    clearTimeout(connectionTimeout);
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    resolve();
                };

                const onError = () => {
                    clearTimeout(connectionTimeout);
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    reject(new Error("WebSocket connection failed"));
                };

                socket.addEventListener("open", onOpen);
                socket.addEventListener("error", onError);
            });
        }

        socket.send("{\"id\":\"auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

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

    async subscribe<T = any>(
        query: string,
        params: any,
        schema: SchemaNode | undefined,
        callbacks: SubscriptionCallbacks<T>
    ): Promise<string> {
        const id = `sub-${this.nextId++}`;

        const request: SubscribeRequest = {
            id,
            type: "Subscribe",
            payload: {query}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, (response) => {
                if (response.type === "Err") {
                    reject(new ReifyError(response));
                } else if (response.type === "Subscribed") {
                    const subscriptionId = response.payload.subscription_id;

                    // Store subscription state
                    this.subscriptions.set(subscriptionId, {
                        subscriptionId,
                        query,
                        params,
                        schema,
                        callbacks
                    });

                    resolve(subscriptionId);
                } else {
                    reject(new Error("Unexpected response type"));
                }
            });

            this.socket.send(JSON.stringify(request));
        });
    }

    async unsubscribe(subscriptionId: string): Promise<void> {
        const id = `unsub-${this.nextId++}`;

        const request: UnsubscribeRequest = {
            id,
            type: "Unsubscribe",
            payload: {subscription_id: subscriptionId}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, (response) => {
                if (response.type === "Err") {
                    reject(new ReifyError(response));
                } else if (response.type === "Unsubscribed") {
                    this.subscriptions.delete(subscriptionId);
                    resolve();
                } else {
                    reject(new Error("Unexpected response type"));
                }
            });

            this.socket.send(JSON.stringify(request));
        });
    }

    async send(req: CommandRequest | QueryRequest): Promise<any> {
        const id = req.id;

        if (this.socket.readyState !== 1) {
            throw new ReifyError({
                id: "connection-error",
                type: "Err",
                payload: {
                    diagnostic: {
                        code: "CONNECTION_LOST",
                        message: "Connection lost",
                        notes: []
                    }
                }
            });
        }

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
                    // Check if it's a Value instance by checking for valueOf method
                    if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                        const rawValue = (value as any).valueOf();
                        transformedRow[key] = this.coerceToPrimitiveType(rawValue, propertySchema.type);
                    } else {
                        transformedRow[key] = this.coerceToPrimitiveType(value, propertySchema.type);
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
            // Check if it's a Value instance by checking for valueOf method
            if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
                return this.coerceToPrimitiveType(row.valueOf(), resultSchema.type);
            }
            return this.coerceToPrimitiveType(row, resultSchema.type);
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

    /**
     * Coerce a value to the expected primitive type based on schema.
     * This handles cases where the server returns a smaller integer type
     * but the schema expects a bigint type (Int8, Int16, Uint8, Uint16).
     */
    private coerceToPrimitiveType(value: any, schemaType: string): any {
        if (value === undefined || value === null) {
            return value;
        }

        // Bigint types: Int8, Int16, Uint8, Uint16
        const bigintTypes = ['Int8', 'Int16', 'Uint8', 'Uint16'];
        if (bigintTypes.includes(schemaType)) {
            if (typeof value === 'bigint') {
                return value;
            }
            if (typeof value === 'number') {
                return BigInt(Math.trunc(value));
            }
            if (typeof value === 'string') {
                return BigInt(value);
            }
        }

        return value;
    }

    disconnect() {
        this.shouldReconnect = false;
        this.subscriptions.clear();
        this.socket.close();
    }

    private handleDisconnect() {
        this.rejectAllPendingRequests();

        if (!this.shouldReconnect || this.isReconnecting) {
            return;
        }

        const maxAttempts = this.options.maxReconnectAttempts ?? 5;
        if (this.reconnectAttempts >= maxAttempts) {
            console.error(`Max reconnection attempts (${maxAttempts}) reached`);
            return;
        }

        this.attemptReconnect();
    }

    private async attemptReconnect() {
        this.isReconnecting = true;
        this.reconnectAttempts++;

        const baseDelay = this.options.reconnectDelayMs ?? 1000;
        const delay = baseDelay * Math.pow(2, this.reconnectAttempts - 1);

        console.log(`Attempting reconnection in ${delay}ms`);

        await new Promise(resolve => setTimeout(resolve, delay));

        try {
            const socket = await createWebSocket(this.options.url);

            if (socket.readyState !== 1) {
                const connectionTimeoutMs = 30000; // 30 second connection timeout
                await new Promise<void>((resolve, reject) => {
                    const connectionTimeout = setTimeout(() => {
                        socket.removeEventListener("open", onOpen);
                        socket.removeEventListener("error", onError);
                        socket.close();
                        reject(new Error(`WebSocket reconnection timeout after ${connectionTimeoutMs}ms`));
                    }, connectionTimeoutMs);

                    const onOpen = () => {
                        clearTimeout(connectionTimeout);
                        socket.removeEventListener("open", onOpen);
                        socket.removeEventListener("error", onError);
                        resolve();
                    };

                    const onError = () => {
                        clearTimeout(connectionTimeout);
                        socket.removeEventListener("open", onOpen);
                        socket.removeEventListener("error", onError);
                        reject(new Error("WebSocket connection failed"));
                    };

                    socket.addEventListener("open", onOpen);
                    socket.addEventListener("error", onError);
                });
            }

            socket.send("{\"id\":\"auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

            this.socket = socket;
            this.setupSocketHandlers();
            this.reconnectAttempts = 0;
            this.isReconnecting = false;

            // Re-establish all active subscriptions
            await this.resubscribeAll();
        } catch (error) {
            this.isReconnecting = false;
            this.handleDisconnect();
        }
    }

    private async resubscribeAll(): Promise<void> {
        const subscriptionsToReestablish = Array.from(this.subscriptions.values());

        // Clear current subscriptions map (will be repopulated)
        this.subscriptions.clear();

        for (const state of subscriptionsToReestablish) {
            try {
                // Re-subscribe with same parameters
                // Cast to avoid overload resolution issues in internal call
                await (this.subscribe as any)(state.query, state.params, state.schema, state.callbacks);
            } catch (err) {
                console.error(`Failed to resubscribe to ${state.query}:`, err);
            }
        }
    }

    private handleChangeMessage(msg: ChangeMessage): void {
        const {subscription_id, frame} = msg.payload;
        const state = this.subscriptions.get(subscription_id);

        if (!state) {
            console.error('No state for subscription_id:', subscription_id);
            return;
        }

        // Extract _op column to determine operation type
        const opColumn = frame.columns.find(c => c.name === "_op");
        if (!opColumn || opColumn.data.length === 0) {
            console.error('Missing or empty _op column:', { opColumn, frame });
            return;
        }

        // Transform frame to rows using existing transformResult logic
        const rows = this.frameToRows(frame, state.schema);

        // Group rows by operation type (defensive - usually all same type)
        // Process in order to maintain sequential execution
        const batches: Array<{ op: 'INSERT' | 'UPDATE' | 'REMOVE'; rows: any[] }> = [];

        for (let i = 0; i < rows.length; i++) {
            const opValue = parseInt(opColumn.data[i]);
            const operation: 'INSERT' | 'UPDATE' | 'REMOVE' =
                opValue === 1 ? 'INSERT' :
                    opValue === 2 ? 'UPDATE' :
                        opValue === 3 ? 'REMOVE' : 'INSERT';

            // Remove _op from this row
            const {_op, ...cleanRow} = rows[i];

            // Batch consecutive rows of same operation type
            if (batches.length > 0 && batches[batches.length - 1].op === operation) {
                batches[batches.length - 1].rows.push(cleanRow);
            } else {
                batches.push({op: operation, rows: [cleanRow]});
            }
        }

        // Execute callbacks sequentially in order
        for (const batch of batches) {
            switch (batch.op) {
                case 'INSERT':
                    state.callbacks.onInsert?.(batch.rows);
                    break;
                case 'UPDATE':
                    state.callbacks.onUpdate?.(batch.rows);
                    break;
                case 'REMOVE':
                    state.callbacks.onRemove?.(batch.rows);
                    break;
            }
        }
    }

    private frameToRows(frame: any, schema?: SchemaNode): any[] {
        // Convert frame columns to array of row objects
        if (!frame.columns || frame.columns.length === 0) return [];

        const rowCount = frame.columns[0].data.length;
        const rows: any[] = [];

        for (let i = 0; i < rowCount; i++) {
            const row: any = {};
            for (const col of frame.columns) {
                row[col.name] = decode({type: col.type, value: col.data[i]});
            }
            rows.push(row);
        }

        // Apply schema transformation if provided
        if (schema) {
            return rows.map(row => this.transformResult(row, schema));
        }

        return rows;
    }

    private setupSocketHandlers() {
        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);

            // Handle server-initiated messages (no id)
            if (!msg.id) {
                if (msg.type === "Change") {
                    this.handleChangeMessage(msg);
                }
                return;
            }

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

        this.socket.onclose = () => {
            this.handleDisconnect();
        };
    }

    private rejectAllPendingRequests() {
        const error: ErrorResponse = {
            id: "connection-error",
            type: "Err",
            payload: {
                diagnostic: {
                    code: "CONNECTION_LOST",
                    message: "Connection lost",
                    notes: []
                }
            }
        };

        for (const handler of this.pending.values()) {
            handler(error);
        }
        this.pending.clear();
    }
}


function columnsToRows(columns: Column[]): Record<string, Value>[] {
    const rowCount = columns[0]?.data.length ?? 0;
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.type, value: col.data[i]});
        }
        return row;
    });
}