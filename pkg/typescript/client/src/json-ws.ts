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
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";

export interface JsonWsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    maxReconnectAttempts?: number;
    reconnectDelayMs?: number;
    unwrap?: boolean;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | LogoutResponse;

async function createWebSocket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    } else {
        //@ts-ignore
        const wsModule = await import("ws");
        return new wsModule.WebSocket(url);
    }
}

export class JsonWebsocketClient {
    private options: JsonWsClientOptions;
    private nextId: number;
    private socket: WebSocket;
    private pending = new Map<string, (response: ResponsePayload) => void>();
    private reconnectAttempts: number = 0;
    private shouldReconnect: boolean = true;
    private isReconnecting: boolean = false;

    private constructor(socket: WebSocket, options: JsonWsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.setupSocketHandlers();
    }

    static async connect(options: JsonWsClientOptions): Promise<JsonWebsocketClient> {
        const socket = await createWebSocket(options.url);

        if (socket.readyState !== 1) {
            const connectionTimeoutMs = 30000;
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

        if (options.token) {
            socket.send(JSON.stringify({id: "auth-1", type: "Auth", payload: {token: options.token}}));
        }

        return new JsonWebsocketClient(socket, options);
    }

    async admin(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.nextId++}`;

        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send({
            id,
            type: "Admin",
            payload: {
                statements: outputStatements,
                params: encodedParams,
                format: "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        });
    }

    async command(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.nextId++}`;

        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send({
            id,
            type: "Command",
            payload: {
                statements: outputStatements,
                params: encodedParams,
                format: "json",
                ...(this.options.unwrap ? {unwrap: true} : {}),
            },
        });
    }

    async query(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const id = `req-${this.nextId++}`;

        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send({
            id,
            type: "Query",
            payload: {
                statements: outputStatements,
                params: encodedParams,
                format: "json",
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
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, timeoutMs);

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

        return response.payload.body;
    }

    async loginWithPassword(identity: string, password: string): Promise<LoginResult> {
        return this.login("password", identity, {password});
    }

    async loginWithToken(identity: string, token: string): Promise<LoginResult> {
        return this.login("token", identity, {token});
    }

    async login(method: string, identity: string, credentials: Record<string, string>): Promise<LoginResult> {
        const id = `auth-${this.nextId++}`;

        const request: AuthRequest = {
            id,
            type: "Auth",
            payload: {method, credentials: {identifier: identity, ...credentials}}
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Login timeout"));
            }, timeoutMs);

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

        const id = `logout-${this.nextId++}`;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeoutMs = this.options.timeoutMs ?? 30_000;
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("Logout timeout"));
            }, timeoutMs);

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
                socket.send(JSON.stringify({id: "auth-1", type: "Auth", payload: {token: this.options.token}}));
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
            const msg = JSON.parse(event.data);

            if (!msg.id) {
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
