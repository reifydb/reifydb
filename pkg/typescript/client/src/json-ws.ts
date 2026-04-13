// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {decode} from "@reifydb/core";
import type {
    AdminRequest,
    AuthRequest,
    AuthResponse,
    CommandRequest,
    QueryRequest,
    AdminResponse,
    Column,
    CommandResponse,
    QueryResponse,
    ErrorResponse,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";
import {rbcf} from "./rbcf";

export interface JsonWsClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    max_reconnect_attempts?: number;
    reconnect_delay_ms?: number;
    unwrap?: boolean;
    signal?: AbortSignal;
    /** Wire-format encoding for data frames. Defaults to "json". */
    encoding?: "json" | "rbcf";
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | LogoutResponse;

interface PendingEntry {
    type: string;
    handler: (response: ResponsePayload) => void;
}

async function create_web_socket(url: string): Promise<WebSocket> {
    let socket: WebSocket;
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        socket = new WebSocket(url);
    } else {
        //@ts-ignore
        const ws_module = await import("ws");
        socket = new ws_module.WebSocket(url);
    }
    try {
        (socket as any).binaryType = "arraybuffer";
    } catch {
        // Some environments disallow setting before open — best effort.
    }
    return socket;
}

export class JsonWebsocketClient {
    private options: JsonWsClientOptions;
    private next_id: number;
    private socket: WebSocket;
    private pending = new Map<string, PendingEntry>();
    private reconnect_attempts: number = 0;
    private should_reconnect: boolean = true;
    private is_reconnecting: boolean = false;

    private constructor(socket: WebSocket, options: JsonWsClientOptions) {
        this.options = options;
        this.next_id = 1;
        this.socket = socket;

        this.setup_socket_handlers();
    }

    static async connect(options: JsonWsClientOptions): Promise<JsonWebsocketClient> {
        if (options.signal?.aborted) {
            throw new Error("AbortError");
        }
        
        const socket = await create_web_socket(options.url);

        if (socket.readyState !== 1) {
            const connection_timeout_ms = 30000;
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

        return new JsonWebsocketClient(socket, options);
    }

    async admin(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.next_id++}`;

        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send({
            id,
            type: "Admin",
            payload: {
                statements: output_statements,
                params: encoded_params,
                format: this.options.encoding === "rbcf" ? "rbcf" : "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        });
    }

    async command(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.next_id++}`;

        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send({
            id,
            type: "Command",
            payload: {
                statements: output_statements,
                params: encoded_params,
                format: this.options.encoding === "rbcf" ? "rbcf" : "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        });
    }

    async query(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.next_id++}`;

        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send({
            id,
            type: "Query",
            payload: {
                statements: output_statements,
                params: encoded_params,
                format: this.options.encoding === "rbcf" ? "rbcf" : "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
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

            this.pending.set(id, {
                type: req.type,
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
            });

            this.socket.send(JSON.stringify(req));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== req.type) {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        return response.payload.body;
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

            this.pending.set(id, {
                type: "Auth",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
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

            this.pending.set(id, {
                type: "Logout",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
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
                const connection_timeout_ms = 30000;
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
        } catch (error) {
            this.is_reconnecting = false;
            this.handle_disconnect();
        }
    }

    private setup_socket_handlers() {
        this.socket.onmessage = (event) => {
            const data = event.data;

            if (data instanceof ArrayBuffer) {
                this.handle_binary_message(new Uint8Array(data));
                return;
            }
            if (typeof data !== "string") {
                const buf = data as { buffer?: ArrayBuffer; byteOffset?: number; byteLength?: number };
                if (buf && typeof buf.byteLength === "number" && buf.buffer instanceof ArrayBuffer) {
                    const u8 = new Uint8Array(buf.buffer, buf.byteOffset ?? 0, buf.byteLength);
                    this.handle_binary_message(u8);
                    return;
                }
                return;
            }

            const msg = JSON.parse(data);

            if (!msg.id) {
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

    private handle_binary_message(bytes: Uint8Array) {
        // Envelope: [u32 LE id_len][id UTF-8 bytes][RBCF payload]. Mirrors ws.ts.
        if (bytes.length < 4) return;
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        const id_len = view.getUint32(0, true);
        if (bytes.length < 4 + id_len) return;
        const id = new TextDecoder("utf-8").decode(bytes.subarray(4, 4 + id_len));
        const rbcf_bytes = bytes.subarray(4 + id_len);

        const entry = this.pending.get(id);
        if (!entry) return;
        this.pending.delete(id);

        let wire_frames: any[];
        try {
            wire_frames = rbcf.decode(rbcf_bytes);
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            entry.handler({
                id,
                type: "Err",
                payload: {
                    diagnostic: {code: "RBCF_DECODE", message: msg, notes: []}
                }
            } as ErrorResponse);
            return;
        }

        // Convert columns to rows with plain JS values to match the format=json body shape
        // (array of frames of plain rows) that JsonWebsocketClient.send returns directly.
        const plain_frames = wire_frames.map((frame: any) => columns_to_plain_rows(frame.columns));
        entry.handler({
            id,
            type: entry.type,
            payload: {body: plain_frames},
        } as ResponsePayload);
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
}

function columns_to_plain_rows(columns: Column[]): Record<string, any>[] {
    const row_count = columns[0]?.payload.length ?? 0;
    return Array.from({length: row_count}, (_, i) => {
        const row: Record<string, any> = {};
        for (const col of columns) {
            const value = decode({type: col.type, value: col.payload[i]});
            row[col.name] = value?.valueOf();
        }
        return row;
    });
}
