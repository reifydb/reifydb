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
    LoginChallengeResult,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
    ResponseMeta,
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
import {rbcf} from "./rbcf";
import {CONTENT_TYPE_RBCF} from "./content-types";

const enum BinaryKind {
    Response = 0x00,
    Change = 0x01,
}

interface BinaryEnvelope {
    kind: BinaryKind;
    id: string;
    meta?: ResponseMeta;
    rbcf: Uint8Array;
}

// Wire format: `[u8 kind][u32 LE id_len][id UTF-8 bytes][u32 LE meta_len][meta UTF-8 JSON bytes][RBCF payload]`.
// Must stay in sync with `encode_rbcf_envelope` in
// `crates/sub-server-ws/src/handler.rs`.
function decode_envelope(bytes: Uint8Array): BinaryEnvelope | null {
    if (bytes.length < 5) return null;
    const kind = bytes[0] as BinaryKind;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const id_len = view.getUint32(1, true);
    if (bytes.length < 5 + id_len + 4) return null;
    const decoder = new TextDecoder("utf-8");
    const id = decoder.decode(bytes.subarray(5, 5 + id_len));

    const meta_len = view.getUint32(5 + id_len, true);
    if (bytes.length < 5 + id_len + 4 + meta_len) return null;

    let meta: ResponseMeta | undefined;
    if (meta_len > 0) {
        const meta_json = decoder.decode(bytes.subarray(5 + id_len + 4, 5 + id_len + 4 + meta_len));
        try {
            meta = JSON.parse(meta_json);
        } catch (e) {
            console.error("Failed to parse RBCF metadata", e);
        }
    }

    const rbcf = bytes.subarray(5 + id_len + 4 + meta_len);
    return {kind, id, meta, rbcf};
}

export interface WsClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    max_reconnect_attempts?: number;
    reconnect_delay_ms?: number;
    signal?: AbortSignal;
    /**
     * Wire format for data frames. Defaults to `"frames"`.
     *
     * - `"json"`   — rows-shape JSON: `[[{col: val, ...}, ...], ...]`
     * - `"frames"` — frames-shape JSON: columnar frames (default)
     * - `"rbcf"`   — frames-shape binary (RBCF)
     */
    format?: "json" | "frames" | "rbcf";
}

interface SubscriptionState<T = any> {
    subscription_id: string;
    rql: string;
    params?: any;
    shape?: ShapeNode;
    callbacks: SubscriptionCallbacks<T>;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | SubscribedResponse | UnsubscribedResponse | LogoutResponse;

async function create_web_socket(url: string): Promise<WebSocket> {
    let socket: WebSocket;
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        socket = new WebSocket(url);
    } else {
        //@ts-ignore
        const ws_module = await import("ws");
        socket = new ws_module.WebSocket(url);
    }
    // Deliver binary frames as ArrayBuffer so the RBCF envelope parser sees a uniform shape
    // across the browser native WebSocket and the node `ws` package.
    try {
        (socket as any).binaryType = "arraybuffer";
    } catch {
        // Some environments disallow setting before open — best effort.
    }
    return socket;
}

interface PendingEntry {
    type: string;
    handler: (response: ResponsePayload) => void;
}


export class WsClient {
    private options: WsClientOptions;
    private next_id: number;
    private socket: WebSocket;
    private pending = new Map<string, PendingEntry>();
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
     * @param rql - RQL string to execute
     * @param params - Parameters for the statements (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async admin<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.admin_with_meta(rql, params, shapes);
        return frames;
    }

    async admin_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Admin", rql, params, shapes);
    }

    /**
     * Execute command(s) with shapes for each statement for proper type inference
     * @param rql - RQL string to execute
     * @param params - Parameters for the commands (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async command<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.command_with_meta(rql, params, shapes);
        return frames;
    }

    async command_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Command", rql, params, shapes);
    }


    /**
     * Execute query(s) with shapes for each statement for proper type inference
     * @param rql - RQL string to execute
     * @param params - Parameters for the queries (use null or {} if no params)
     * @param shapes - Shape for each statement's result
     */
    async query<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.query_with_meta(rql, params, shapes);
        return frames;
    }

    async query_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Query", rql, params, shapes);
    }

    private async execute<const S extends readonly ShapeNode[]>(
        type: "Admin" | "Command" | "Query",
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        const id = `req-${this.next_id++}`;

        // Encode params without shape assumptions
        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const { result, meta } = await this.send_with_meta({
            id,
            type,
            payload: {
                rql,
                params: encoded_params
            },
        } as AdminRequest | CommandRequest | QueryRequest);

        // Transform each frame with its corresponding shape
        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame; // No shape for this frame, return as-is
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return { frames: transformed_frames as FrameResults<S>, meta };
    }

    async subscribe<T = any>(
        rql: string,
        params: any,
        shape: ShapeNode | undefined,
        callbacks: SubscriptionCallbacks<T>
    ): Promise<string> {
        const id = `sub-${this.next_id++}`;

        // Subscriptions always use columnar shape (frames or rbcf) — the change-tracking
        // protocol reads `_op` from the columnar layout, so rows-shape JSON cannot carry it.
        const sub_format = this.options.format === "rbcf" ? "rbcf" : "frames";
        const request: SubscribeRequest = {
            id,
            type: "Subscribe",
            payload: {rql, format: sub_format} as any
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "Subscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "Subscribed") {
                        const subscription_id = response.payload.subscription_id;

                        // Store subscription state
                        this.subscriptions.set(subscription_id, {
                            subscription_id,
                            rql,
                            params,
                            shape,
                            callbacks
                        });

                        resolve(subscription_id);
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
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
            this.pending.set(id, {
                type: "Unsubscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "Unsubscribed") {
                        this.subscriptions.delete(subscription_id);
                        resolve();
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
                }
            });

            this.socket.send(JSON.stringify(request));
        });
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest): Promise<any> {
        const { result } = await this.send_with_meta(req);
        return result;
    }

    async send_with_meta(
        req: AdminRequest | CommandRequest | QueryRequest,
    ): Promise<{ result: any, meta?: ResponseMeta }> {
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

        req = {
            ...req,
            payload: { ...req.payload, format: this.wire_format() },
        } as AdminRequest | CommandRequest | QueryRequest;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, timeout_ms);

            this.pending.set(id, {
                type: req.type,
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                }
            });

            this.socket.send(JSON.stringify(req));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);

        }

        if (response.type !== req.type) {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        const meta = (response.payload as any).meta as ResponseMeta | undefined;

        // Response shape depends on wire format:
        // - "json"   → body is `[[{col: val}, ...], ...]` already in rows shape
        // - "frames" → body is `{frames: [ColumnarFrame, ...]}` needing column→row pivot
        // - "rbcf"   → handle_binary_message synthesizes `{frames}` so it matches "frames"
        if (this.wire_format() === "json") {
            return { result: response.payload.body ?? [], meta };
        }
        const frames = response.payload.body?.frames || [];
        return {
            result: frames.map((frame: any) => columns_to_rows(frame.columns)),
            meta,
        };
    }

    private wire_format(): "json" | "frames" | "rbcf" {
        return this.options.format ?? "frames";
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
        return this.login("password", {identifier: identity, password});
    }

    async login_with_token(token: string): Promise<LoginResult> {
        return this.login("token", {token});
    }

    async login(method: string, credentials: Record<string, string>): Promise<LoginResult> {
        const id = `auth-${this.next_id++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeout_ms);

            this.pending.set(id, {
                type: "Auth",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                }
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

    async login_challenge(method: string, credentials: Record<string, string>): Promise<LoginChallengeResult> {
        const id = `auth-${this.next_id++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout_ms = this.options.timeout_ms ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeout_ms);

            this.pending.set(id, {
                type: "Auth",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                }
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

        if (payload.status === "challenge") {
            if (!payload.challenge_id || !payload.payload?.message || !payload.payload?.nonce) {
                throw new Error("Malformed challenge response");
            }
            return {
                kind: "challenge",
                challenge_id: payload.challenge_id,
                message: payload.payload.message,
                nonce: payload.payload.nonce,
            };
        }

        if (payload.status === "authenticated" && payload.token && payload.identity) {
            this.options.token = payload.token;
            return {kind: "authenticated", token: payload.token, identity: payload.identity};
        }

        throw new Error(`Authentication failed: ${payload.reason ?? "unknown"}`);
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

            this.pending.set(id, {
                type: "Logout",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                }
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
                await (this.subscribe as any)(state.rql, state.params, state.shape, state.callbacks);
            } catch (err) {
                console.error(`Failed to resubscribe to ${state.rql}:`, err);
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
            const data = event.data;

            // Binary path: RBCF envelope [u32 LE id_len][id UTF-8 bytes][RBCF payload].
            // Only Admin/Command/Query responses arrive as binary; errors and subscription
            // pushes are always JSON text.
            if (data instanceof ArrayBuffer) {
                this.handle_binary_message(new Uint8Array(data));
                return;
            }
            if (typeof data !== "string") {
                // Node `ws` without binaryType setting — convert Buffer-like to ArrayBuffer.
                const buf = data as { buffer?: ArrayBuffer; byteOffset?: number; byteLength?: number };
                if (buf && typeof buf.byteLength === "number" && buf.buffer instanceof ArrayBuffer) {
                    const u8 = new Uint8Array(buf.buffer, buf.byteOffset ?? 0, buf.byteLength);
                    this.handle_binary_message(u8);
                    return;
                }
                return;
            }

            const msg = JSON.parse(data);

            // Handle server-initiated messages (no id)
            if (!msg.id) {
                if (msg.type === "Change") {
                    this.handle_change_message(msg);
                }
                return;
            }

            const {id, type, payload} = msg;

            const entry = this.pending.get(id);
            if (!entry) {
                return;
            }

            this.pending.delete(id);
            entry.handler({id, type, payload});
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

        for (const entry of this.pending.values()) {
            entry.handler(error);
        }
        this.pending.clear();
    }

    private handle_binary_message(bytes: Uint8Array) {
        const envelope = decode_envelope(bytes);
        if (!envelope) return;
        const {kind, id, rbcf: rbcf_bytes} = envelope;

        let frames: any[];
        try {
            frames = rbcf.decode(rbcf_bytes);
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            if (kind === BinaryKind.Response) {
                const entry = this.pending.get(id);
                if (!entry) return;
                this.pending.delete(id);
                entry.handler({
                    id,
                    type: "Err",
                    payload: {
                        diagnostic: { code: "RBCF_DECODE", message: msg, notes: [] }
                    }
                } as ErrorResponse);
            } else {
                console.error(`Failed to decode RBCF change for subscription ${id}: ${msg}`);
            }
            return;
        }

        if (kind === BinaryKind.Response) {
            const entry = this.pending.get(id);
            if (!entry) return;
            this.pending.delete(id);
            // Synthesize a response that looks like the JSON path so downstream logic is unchanged.
            entry.handler({
                id,
                type: entry.type,
                payload: {
                    content_type: CONTENT_TYPE_RBCF,
                    body: { frames },
                    meta: envelope.meta,
                },
            } as ResponsePayload);
            return;
        }

        if (kind === BinaryKind.Change) {
            // Feed decoded frames through the same dispatch path as JSON change pushes.
            this.handle_change_message({
                type: "Change",
                payload: {
                    subscription_id: id,
                    content_type: CONTENT_TYPE_RBCF,
                    body: { frames },
                }
            });
        }
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