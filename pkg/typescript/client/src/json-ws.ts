// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type {
    AdminRequest,
    AuthRequest,
    AuthResponse,
    CommandRequest,
    QueryRequest,
    AdminResponse,
    CommandResponse,
    QueryResponse,
    ErrorResponse,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
    ResponseMeta,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";

export interface JsonWsClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    max_reconnect_attempts?: number;
    reconnect_delay_ms?: number;
    unwrap?: boolean;
    signal?: AbortSignal;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | LogoutResponse;

interface PendingEntry {
    type: string;
    handler: (response: ResponsePayload) => void;
}

async function create_web_socket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    }
    //@ts-ignore
    const ws_module = await import("ws");
    return new ws_module.WebSocket(url);
}

export class JsonWsClient {
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

    static async connect(options: JsonWsClientOptions): Promise<JsonWsClient> {
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

        return new JsonWsClient(socket, options);
    }

    /**
     * @param rql - RQL string to execute
     */
    async admin(
        rql: string,
        params?: any,
    ): Promise<any> {
        const { data } = await this.admin_with_meta(rql, params);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async admin_with_meta(
        rql: string,
        params?: any,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Admin", rql, params);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command(
        rql: string,
        params?: any,
    ): Promise<any> {
        const { data } = await this.command_with_meta(rql, params);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async command_with_meta(
        rql: string,
        params?: any,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Command", rql, params);
    }

    /**
     * @param rql - RQL string to execute
     */
    async query(
        rql: string,
        params?: any,
    ): Promise<any> {
        const { data } = await this.query_with_meta(rql, params);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async query_with_meta(
        rql: string,
        params?: any,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Query", rql, params);
    }

    private async execute(
        type: "Admin" | "Command" | "Query",
        rql: string,
        params?: any,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const id = `req-${this.next_id++}`;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send_with_meta({
            id,
            type,
            payload: {
                rql,
                params: encoded_params,
                format: "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        } as AdminRequest | CommandRequest | QueryRequest);
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest): Promise<any> {
        const { data } = await this.send_with_meta(req);
        return data;
    }

    async send_with_meta(
        req: AdminRequest | CommandRequest | QueryRequest,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
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

        const meta = (response.payload as any).meta as ResponseMeta | undefined;
        return { data: response.payload.body, meta };
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
            if (typeof data !== "string") return;

            const msg = JSON.parse(data);
            if (!msg.id) return;

            const {id, type, payload} = msg;
            const entry = this.pending.get(id);
            if (!entry) return;

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
}
