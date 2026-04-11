// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {
    decode,
    Value
} from "@reifydb/core";
import type {
    ShapeNode,
    FrameResults,
} from "@reifydb/core";

import type {
    AdminRequest,
    AdminResponse,
    AuthRequest,
    AuthResponse,
    CommandRequest,
    CommandResponse,
    QueryRequest,
    QueryResponse,
    Column,
    ErrorResponse,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
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
import {encode_params} from "./encoder";

export interface WsClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    max_reconnect_attempts?: number;
    reconnect_delay_ms?: number;
    signal?: AbortSignal;
}

interface SubscriptionState<T = any> {
    subscription_id: string;
    query: string;
    params?: any;
    shape?: ShapeNode;
    callbacks: SubscriptionCallbacks<T>;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | SubscribedResponse | UnsubscribedResponse | LogoutResponse;

async function create_web_socket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    } else {
        //@ts-ignore
        const ws_module = await import("ws");
        return new ws_module.WebSocket(url);
    }
}


export class WsClient {
    private options: WsClientOptions;
    private next_id: number;
    private socket: WebSocket;
    private pending = new Map<string, (response: ResponsePayload) => void>();
    private reconnect_attempts: number = 0;
    private should_reconnect: boolean = true;
    private is_reconnecting: boolean = false;
    private subscriptions = new Map<string, SubscriptionState>();

    private constructor(socket: WebSocket, options: WsClientOptions) {
        this.options = options;
        this.next_id = 1;
        this.socket = socket;

        this.setup_socket_handlers();
    }

    static async connect(options: WsClientOptions): Promise<WsClient> {
        if (options.signal?.aborted) {
            throw new Error("AbortError");
        }

        const socket = await create_web_socket(options.url);

        // Wait for connection to open if not already open, with timeout
        if (socket.readyState !== 1) {
            const connection_timeout_ms = 30000; // 30 second connection timeout
            await new Promise<void>((resolve, reject) => {
                const connection_timeout = setTimeout(() => {
                    cleanup();
                    socket.close();
                    reject(new Error(`WebSocket connection timeout after ${connection_timeout_ms}ms`));
                }, connection_timeout_ms);

                const on_abort = () => {
                    cleanup();
                    socket.close();
                    reject(new Error("AbortError"));
                };

                const on_open = () => {
                    cleanup();
                    resolve();
                };

                const on_error = () => {
                    cleanup();
                    reject(new Error("WebSocket connection failed"));
                };

                const cleanup = () => {
                    clearTimeout(connection_timeout);
                    socket.removeEventListener("open", on_open);
                    socket.removeEventListener("error", on_error);
                    if (options.signal) {
                        options.signal.removeEventListener("abort", on_abort);
                    }
                };

                if (options.signal) {
                    options.signal.addEventListener("abort", on_abort);
                }
                
                socket.addEventListener("open", on_open);
                socket.addEventListener("error", on_error);
            });
        }

        if (options.signal?.aborted) {
            socket.close();
            throw new Error("AbortError");
        }

        if (options.token) {
            socket.send(JSON.stringify({id: "auth-1", type: "Auth", payload: {token: options.token}}));
        }

        return new WsClient(socket, options);
    }

    /**
     * Execute admin operation(s) with shapes for each statement for proper type inference.
     * Admin operations support DDL (CREATE TABLE, ALTER, etc.), DML, and queries.
     * @param statements - Single statement or array of RQL statements
     * @param params - Parameters for the statements (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async admin<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const id = `req-${this.next_id++}`;

        // Normalize statements to array
        const statement_array = Array.isArray(statements) ? statements : [statements];
        // When multiple array elements, mark each with OUTPUT so results are returned.
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        // Encode params without shape assumptions
        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send({
            id,
            type: "Admin",
            payload: {
                statements: output_statements,
                params: encoded_params
            },
        });

        // Transform each frame with its corresponding shape
        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame; // No shape for this frame, return as-is
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
    }

    /**
     * Execute command(s) with shapes for each statement for proper type inference
     * @param statements - Single statement or array of RQL commands
     * @param params - Parameters for the commands (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async command<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const id = `req-${this.next_id++}`;

        // Normalize statements to array
        const statement_array = Array.isArray(statements) ? statements : [statements];
        // When multiple array elements, mark each with OUTPUT so results are returned.
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        // Encode params without shape assumptions
        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send({
            id,
            type: "Command",
            payload: {
                statements: output_statements,
                params: encoded_params
            },
        });

        // Transform each frame with its corresponding shape
        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame; // No shape for this frame, return as-is
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
    }


    /**
     * Execute query(s) with shapes for each statement for proper type inference
     * @param statements - Single statement or array of RQL queries
     * @param params - Parameters for the queries (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async query<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const id = `req-${this.next_id++}`;

        // Normalize statements to array
        const statement_array = Array.isArray(statements) ? statements : [statements];
        // When multiple array elements, mark each with OUTPUT so results are returned.
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        // Encode params without shape assumptions
        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send({
            id,
            type: "Query",
            payload: {
                statements: output_statements,
                params: encoded_params
            },
        });

        // Transform each frame with its corresponding shape
        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame; // No shape for this frame, return as-is
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
    }

    async subscribe<T = any>(
        query: string,
        params: any,
        shape: ShapeNode | undefined,
        callbacks: SubscriptionCallbacks<T>
    ): Promise<string> {
        const id = `sub-${this.next_id++}`;

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
                    const subscription_id = response.payload.subscription_id;

                    // Store subscription state
                    this.subscriptions.set(subscription_id, {
                        subscription_id,
                        query,
                        params,
                        shape,
                        callbacks
                    });

                    resolve(subscription_id);
                } else {
                    reject(new Error("Unexpected response type"));
                }
            });

            this.socket.send(JSON.stringify(request));
        });
    }

    async unsubscribe(subscription_id: string): Promise<void> {
        const id = `unsub-${this.next_id++}`;

        const request: UnsubscribeRequest = {
            id,
            type: "Unsubscribe",
            payload: {subscription_id: subscription_id}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, (response) => {
                if (response.type === "Err") {
                    reject(new ReifyError(response));
                } else if (response.type === "Unsubscribed") {
                    this.subscriptions.delete(subscription_id);
                    resolve();
                } else {
                    reject(new Error("Unexpected response type"));
                }
            });

            this.socket.send(JSON.stringify(request));
        });
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest): Promise<any> {
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
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, timeout_ms);

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

        const frames = response.payload.body?.frames || [];
        return frames.map((frame: any) =>
            columns_to_rows(frame.columns)
        );
    }


    private transform_result(row: any, result_shape: any): any {
        // Handle object shape with primitive or value properties
        if (result_shape && result_shape.kind === 'object' && result_shape.properties) {
            const transformed_row: any = {};
            for (const [key, value] of Object.entries(row)) {
                const property_shape = result_shape.properties[key];
                if (property_shape && property_shape.kind === 'primitive') {
                    // Convert Value objects to primitives for primitive shape properties
                    // Check if it's a Value instance by checking for valueOf method
                    if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                        const raw_value = (value as any).valueOf();
                        transformed_row[key] = this.coerce_to_primitive_type(raw_value, property_shape.type);
                    } else {
                        transformed_row[key] = this.coerce_to_primitive_type(value, property_shape.type);
                    }
                } else if (property_shape && property_shape.kind === 'value') {
                    // Keep Value objects as-is for value shape properties
                    transformed_row[key] = value;
                } else {
                    // Recursively transform nested structures
                    transformed_row[key] = property_shape ? this.transform_result(value, property_shape) : value;
                }
            }
            return transformed_row;
        }

        // Handle primitive shape transformation
        if (result_shape && result_shape.kind === 'primitive') {
            // Single primitive value - extract from Value object if needed
            // Check if it's a Value instance by checking for valueOf method
            if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
                return this.coerce_to_primitive_type(row.valueOf(), result_shape.type);
            }
            return this.coerce_to_primitive_type(row, result_shape.type);
        }

        // Handle value shape transformation - keep Value objects as-is
        if (result_shape && result_shape.kind === 'value') {
            return row;
        }

        // Handle array shape
        if (result_shape && result_shape.kind === 'array') {
            if (Array.isArray(row)) {
                return row.map((item: any) => this.transform_result(item, result_shape.items));
            }
            return row;
        }

        // Handle optional shape
        if (result_shape && result_shape.kind === 'optional') {
            if (row === undefined || row === null) {
                return undefined;
            }
            return this.transform_result(row, result_shape.shape);
        }

        // Default: return as-is
        return row;
    }

    /**
     * Coerce a value to the expected primitive type based on shape.
     * This handles cases where the server returns a smaller integer type
     * but the shape expects a bigint type (Int8, Int16, Uint8, Uint16).
     */
    private coerce_to_primitive_type(value: any, shape_type: string): any {
        if (value === undefined || value === null) {
            return value;
        }

        // Bigint types: Int8, Int16, Uint8, Uint16
        const bigint_types = ['Int8', 'Int16', 'Uint8', 'Uint16'];
        if (bigint_types.includes(shape_type)) {
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

    async login_with_password(identity: string, password: string): Promise<LoginResult> {
        return this.login("password", identity, {password});
    }

    async login_with_token(identity: string, token: string): Promise<LoginResult> {
        return this.login("token", identity, {token});
    }

    async login(method: string, identity: string, credentials: Record<string, string>): Promise<LoginResult> {
        const id = `auth-${this.next_id++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials: {identifier: identity, ...credentials}}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeout_ms);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify(request));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== "Auth") {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        const payload = (response as AuthResponse).payload;
        if (payload.status !== "authenticated" || !payload.token || !payload.identity) {
            throw new Error("Authentication failed");
        }

        this.options.token = payload.token;

        return {token: payload.token, identity: payload.identity};
    }

    async logout(): Promise<void> {
        if (!this.options.token) {
            return;
        }

        const id = `logout-${this.next_id++}`;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Logout timeout"));
            }, timeout_ms);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify({id, type: "Logout"}));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        this.options = {...this.options, token: undefined};
    }

    disconnect() {
        this.should_reconnect = false;
        this.subscriptions.clear();
        this.socket.close();
    }

    private handle_disconnect() {
        this.reject_all_pending_requests();

        if (!this.should_reconnect || this.is_reconnecting) {
            return;
        }

        const max_attempts = this.options.max_reconnect_attempts ?? 5;
        if (this.reconnect_attempts >= max_attempts) {
            console.error(`Max reconnection attempts (${max_attempts}) reached`);
            return;
        }

        this.attempt_reconnect();
    }

    private async attempt_reconnect() {
        this.is_reconnecting = true;
        this.reconnect_attempts++;

        const base_delay = this.options.reconnect_delay_ms ?? 1000;
        const delay = base_delay * Math.pow(2, this.reconnect_attempts - 1);

        console.log(`Attempting reconnection in ${delay}ms`);

        await new Promise(resolve => setTimeout(resolve, delay));

        try {
            const socket = await create_web_socket(this.options.url);

            if (socket.readyState !== 1) {
                const connection_timeout_ms = 30000; // 30 second connection timeout
                await new Promise<void>((resolve, reject) => {
                    const connection_timeout = setTimeout(() => {
                        socket.removeEventListener("open", on_open);
                        socket.removeEventListener("error", on_error);
                        socket.close();
                        reject(new Error(`WebSocket reconnection timeout after ${connection_timeout_ms}ms`));
                    }, connection_timeout_ms);

                    const on_open = () => {
                        clearTimeout(connection_timeout);
                        socket.removeEventListener("open", on_open);
                        socket.removeEventListener("error", on_error);
                        resolve();
                    };

                    const on_error = () => {
                        clearTimeout(connection_timeout);
                        socket.removeEventListener("open", on_open);
                        socket.removeEventListener("error", on_error);
                        reject(new Error("WebSocket connection failed"));
                    };

                    socket.addEventListener("open", on_open);
                    socket.addEventListener("error", on_error);
                });
            }

            if (this.options.token) {
                socket.send(JSON.stringify({id: "auth-1", type: "Auth", payload: {token: this.options.token}}));
            }

            this.socket = socket;
            this.setup_socket_handlers();
            this.reconnect_attempts = 0;
            this.is_reconnecting = false;

            // Re-establish all active subscriptions
            await this.resubscribe_all();
        } catch (error) {
            this.is_reconnecting = false;
            this.handle_disconnect();
        }
    }

    private async resubscribe_all(): Promise<void> {
        const subscriptions_to_reestablish = Array.from(this.subscriptions.values());

        // Clear current subscriptions map (will be repopulated)
        this.subscriptions.clear();

        for (const state of subscriptions_to_reestablish) {
            try {
                // Re-subscribe with same parameters
                // Cast to avoid overload resolution issues in internal call
                await (this.subscribe as any)(state.query, state.params, state.shape, state.callbacks);
            } catch (err) {
                console.error(`Failed to resubscribe to ${state.query}:`, err);
            }
        }
    }

    private handle_change_message(msg: ChangeMessage): void {
        const {subscription_id, body} = msg.payload;
        const state = this.subscriptions.get(subscription_id);

        if (!state) {
            console.error('No state for subscription_id:', subscription_id);
            return;
        }

        const frames = body?.frames || [];
        if (frames.length === 0) return;
        const frame = frames[0];

        // Extract _op column to determine operation type
        const op_column = frame.columns.find((c: any) => c.name === "_op");
        if (!op_column || op_column.payload.length === 0) {
            console.error('Missing or empty _op column:', { op_column, frame });
            return;
        }

        // Transform frame to rows using existing transform_result logic
        const rows = this.frame_to_rows(frame, state.shape);

        // Group rows by operation type (defensive - usually all same type)
        // Process in order to maintain sequential execution
        const batches: Array<{ op: 'INSERT' | 'UPDATE' | 'REMOVE'; rows: any[] }> = [];

        for (let i = 0; i < rows.length; i++) {
            const op_value = parseInt(op_column.payload[i]);
            const operation: 'INSERT' | 'UPDATE' | 'REMOVE' =
                op_value === 1 ? 'INSERT' :
                    op_value === 2 ? 'UPDATE' :
                        op_value === 3 ? 'REMOVE' : 'INSERT';

            // Remove _op from this row
            const {_op, ...clean_row} = rows[i];

            // Batch consecutive rows of same operation type
            if (batches.length > 0 && batches[batches.length - 1].op === operation) {
                batches[batches.length - 1].rows.push(clean_row);
            } else {
                batches.push({op: operation, rows: [clean_row]});
            }
        }

        // Execute callbacks sequentially in order
        for (const batch of batches) {
            switch (batch.op) {
                case 'INSERT':
                    state.callbacks.on_insert?.(batch.rows);
                    break;
                case 'UPDATE':
                    state.callbacks.on_update?.(batch.rows);
                    break;
                case 'REMOVE':
                    state.callbacks.on_remove?.(batch.rows);
                    break;
            }
        }
    }

    private frame_to_rows(frame: any, shape?: ShapeNode): any[] {
        // Convert frame columns to array of row objects
        if (!frame.columns || frame.columns.length === 0) return [];

        const row_count = frame.columns[0].payload.length;
        const rows: any[] = [];

        for (let i = 0; i < row_count; i++) {
            const row: any = {};
            for (const col of frame.columns) {
                row[col.name] = decode({type: col.type, value: col.payload[i]});
            }
            rows.push(row);
        }

        // Apply shape transformation if provided
        if (shape) {
            return rows.map(row => this.transform_result(row, shape));
        }

        return rows;
    }

    private setup_socket_handlers() {
        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);

            // Handle server-initiated messages (no id)
            if (!msg.id) {
                if (msg.type === "Change") {
                    this.handle_change_message(msg);
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
            this.handle_disconnect();
        };
    }

    private reject_all_pending_requests() {
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


function columns_to_rows(columns: Column[]): Record<string, Value>[] {
    const row_count = columns[0]?.payload.length ?? 0;
    return Array.from({length: row_count}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.type, value: col.payload[i]});
        }
        return row;
    });
}