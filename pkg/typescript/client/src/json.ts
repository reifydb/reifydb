// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import type {Params} from "@reifydb/core";
import type {
    AdminRequest,
    CommandRequest,
    QueryRequest,
    ErrorResponse,
} from "./types";
import {ReifyError} from "./types";
import {encodeParams} from "./encoder";

export interface JsonWsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    maxReconnectAttempts?: number;
    reconnectDelayMs?: number;
}

type ResponsePayload = ErrorResponse | { id: string; type: string; payload: { content_type: string; body: any } };

async function createWebSocket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    } else {
        //@ts-ignore
        const wsModule = await import("ws");
        return new wsModule.WebSocket(url);
    }
}

export class JsonWsClient {
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

    static async connect(options: JsonWsClientOptions): Promise<JsonWsClient> {
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

        socket.send("{\"id\":\"auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

        return new JsonWsClient(socket, options);
    }

    async query<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("Query", statements, params);
    }

    async command<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("Command", statements, params);
    }

    async admin<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("Admin", statements, params);
    }

    disconnect() {
        this.shouldReconnect = false;
        this.socket.close();
    }

    private async send<T>(type: "Admin" | "Command" | "Query", statements: string | string[], params?: Params): Promise<T[][]> {
        const id = `req-${this.nextId++}`;

        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

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

            this.socket.send(JSON.stringify({
                id,
                type,
                payload: {
                    statements: outputStatements,
                    params: encodedParams,
                    format: "json",
                },
            }));
        });

        if (response.type === "Err") {
            throw new ReifyError(response as ErrorResponse);
        }

        if (response.type !== type) {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        const body = (response as any).payload.body;

        // body is an array of arrays (one per frame/statement)
        if (Array.isArray(body) && body.length > 0 && Array.isArray(body[0])) {
            return body as T[][];
        }

        // body is a single array of objects (single frame)
        if (Array.isArray(body)) {
            return [body] as T[][];
        }

        // body is a single unwrapped object
        if (body && typeof body === 'object') {
            return [[body]] as T[][];
        }

        return [] as T[][];
    }

    private handleDisconnect() {
        this.rejectAllPendingRequests();

        if (!this.shouldReconnect || this.isReconnecting) {
            return;
        }

        const maxAttempts = this.options.maxReconnectAttempts ?? 5;
        if (this.reconnectAttempts >= maxAttempts) {
            return;
        }

        this.attemptReconnect();
    }

    private async attemptReconnect() {
        this.isReconnecting = true;
        this.reconnectAttempts++;

        const baseDelay = this.options.reconnectDelayMs ?? 1000;
        const delay = baseDelay * Math.pow(2, this.reconnectAttempts - 1);

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

            socket.send("{\"id\":\"auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

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

            const {id} = msg;
            const handler = this.pending.get(id);
            if (!handler) {
                return;
            }

            this.pending.delete(id);
            handler(msg);
        };

        this.socket.onerror = () => {};

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
