// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {
    AdminRequest,
    AuthRequest,
    AuthResponse,
    CommandRequest,
    QueryRequest,
    CallRequest,
    AdminResponse,
    CommandResponse,
    QueryResponse,
    CallResponse,
    ErrorResponse,
    LoginChallengeResult,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
    ResponseMeta,
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";
import {toCamelCaseKeys, toSnakeCaseKeys, WIRE_PASSTHROUGH_KEYS} from "./case";
import {transformFrames} from "@reifydb/core";
import type {ShapeNode} from "@reifydb/core";

export interface JsonWsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    maxReconnectAttempts?: number;
    reconnectDelayMs?: number;
    unwrap?: boolean;
    signal?: AbortSignal;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | CallResponse | LogoutResponse;

interface PendingEntry {
    type: string;
    handler: (response: ResponsePayload) => void;
}

async function createWebSocket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    }
    //@ts-ignore
    const wsModule = await import("ws");
    return new wsModule.WebSocket(url);
}

export class JsonWsClient {
    private options: JsonWsClientOptions;
    private nextId: number;
    private socket: WebSocket;
    private pending = new Map<string, PendingEntry>();
    private reconnectAttempts: number = 0;
    private shouldReconnect: boolean = true;
    private isReconnecting: boolean = false;

    private constructor(socket: WebSocket, options: JsonWsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.setupSocketHandlers();
    }

    static async connect(options: JsonWsClientOptions): Promise<JsonWsClient> {
        if (options.signal?.aborted) {
            throw new Error("AbortError");
        }

        const socket = await createWebSocket(options.url);

        if (socket.readyState !== 1) {
            const connectionTimeoutMs = 30000;
            await new Promise<void>((resolve, reject) => {
                const connectionTimeout = setTimeout(() => {
                    cleanup();
                    socket.close();
                    reject(new Error(`WebSocket connection timeout after ${connectionTimeoutMs}ms`));
                }, connectionTimeoutMs);

                const onAbort = () => {
                    cleanup();
                    socket.close();
                    reject(new Error("AbortError"));
                };

                const onOpen = () => {
                    cleanup();
                    resolve();
                };

                const onError = () => {
                    cleanup();
                    reject(new Error("WebSocket connection failed"));
                };

                const cleanup = () => {
                    clearTimeout(connectionTimeout);
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    if (options.signal) {
                        options.signal.removeEventListener("abort", onAbort);
                    }
                };

                if (options.signal) {
                    options.signal.addEventListener("abort", onAbort);
                }

                socket.addEventListener("open", onOpen);
                socket.addEventListener("error", onError);
            });
        }

        if (options.signal?.aborted) {
            socket.close();
            throw new Error("AbortError");
        }

        if (options.token) {
            socket.send(JSON.stringify(toSnakeCaseKeys({id: "auth-1", type: "Auth", payload: {token: options.token}}, WIRE_PASSTHROUGH_KEYS)));
        }

        return new JsonWsClient(socket, options);
    }

    /**
     * @param rql - RQL string to execute
     */
    async admin(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<any> {
        const { data } = await this.adminWithMeta(rql, params, shapes);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async adminWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Admin", rql, params, shapes);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<any> {
        const { data } = await this.commandWithMeta(rql, params, shapes);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async commandWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Command", rql, params, shapes);
    }

    /**
     * @param rql - RQL string to execute
     */
    async query(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<any> {
        const { data } = await this.queryWithMeta(rql, params, shapes);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async queryWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute("Query", rql, params, shapes);
    }

    /**
     * @param name - globally-unique name of the WS binding to invoke
     */
    async call(
        name: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<any> {
        const { data } = await this.callWithMeta(name, params, shapes);
        return data;
    }

    async callWithMeta(
        name: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const id = `req-${this.nextId++}`;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { data, meta } = await this.sendWithMeta({
            id,
            type: "Call",
            payload: {
                name,
                params: encodedParams,
                format: "json",
            },
        } as CallRequest);

        return { data: transformFrames(data ?? [], shapes ?? []), meta };
    }

    private async execute(
        type: "Admin" | "Command" | "Query",
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const id = `req-${this.nextId++}`;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { data, meta } = await this.sendWithMeta({
            id,
            type,
            payload: {
                rql,
                params: encodedParams,
                format: "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        } as AdminRequest | CommandRequest | QueryRequest);

        return { data: transformFrames(data ?? [], shapes ?? []), meta };
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest | CallRequest): Promise<any> {
        const { data } = await this.sendWithMeta(req);
        return data;
    }

    async sendWithMeta(
        req: AdminRequest | CommandRequest | QueryRequest | CallRequest,
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
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, timeoutMs);

            this.pending.set(id, {
                type: req.type,
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(req, WIRE_PASSTHROUGH_KEYS)));
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

    async loginWithPassword(identity: string, password: string): Promise<LoginResult> {
        return this.login("password", {identifier: identity, password});
    }

    async loginWithToken(token: string): Promise<LoginResult> {
        return this.login("token", {token});
    }

    async login(method: string, credentials: Record<string, string>): Promise<LoginResult> {
        const id = `auth-${this.nextId++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeoutMs);

            this.pending.set(id, {
                type: "Auth",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
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

    async loginChallenge(method: string, credentials: Record<string, string>): Promise<LoginChallengeResult> {
        const id = `auth-${this.nextId++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeoutMs);

            this.pending.set(id, {
                type: "Auth",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== "Auth") {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        const payload = (response as AuthResponse).payload;

        if (payload.status === "challenge") {
            if (!payload.challengeId || !payload.payload?.message || !payload.payload?.nonce) {
                throw new Error("Malformed challenge response");
            }
            return {
                kind: "challenge",
                challengeId: payload.challengeId,
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

        const id = `logout-${this.nextId++}`;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Logout timeout"));
            }, timeoutMs);

            this.pending.set(id, {
                type: "Logout",
                handler: (res) => {
                    clearTimeout(timeout);
                    resolve(res);
                },
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys({id, type: "Logout"}, WIRE_PASSTHROUGH_KEYS)));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        this.options = {...this.options, token: undefined};
    }

    disconnect() {
        this.shouldReconnect = false;
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
                const connectionTimeoutMs = 30000;
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

            if (this.options.token) {
                socket.send(JSON.stringify(toSnakeCaseKeys({id: "auth-1", type: "Auth", payload: {token: this.options.token}}, WIRE_PASSTHROUGH_KEYS)));
            }

            this.socket = socket;
            this.setupSocketHandlers();
            this.reconnectAttempts = 0;
            this.isReconnecting = false;
        } catch (error) {
            this.isReconnecting = false;
            this.handleDisconnect();
        }
    }

    private setupSocketHandlers() {
        this.socket.onmessage = (event) => {
            const data = event.data;
            if (typeof data !== "string") return;

            const msg = toCamelCaseKeys<any>(JSON.parse(data), WIRE_PASSTHROUGH_KEYS);
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

        for (const entry of this.pending.values()) {
            entry.handler(error);
        }
        this.pending.clear();
    }
}
