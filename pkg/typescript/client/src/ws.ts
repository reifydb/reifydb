// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    decode,
    columnsToRows,
    transformFrames,
    transformResult,
    ROW_NUMBER_KEY
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
    CallRequest,
    CallResponse,
    ErrorResponse,
    LoginChallengeResult,
    LoginResult,
    LogoutRequest,
    LogoutResponse,
    ResponseMeta,
    SubscribeRequest,
    SubscribedResponse,
    SubscriptionConfig,
    UnsubscribeRequest,
    UnsubscribedResponse,
    ChangeMessage,
    SubscriptionCallbacks,
    BatchSubscribeRequest,
    BatchSubscribedResponse,
    BatchUnsubscribeRequest,
    BatchUnsubscribedResponse,
    BatchChangeMessage,
    BatchMemberClosedMessage,
    BatchClosedMessage,
    BatchSubscriptionMember,
    BatchSubscriptionCallbacks,
    BatchSubscription
} from "./types";
import {
    buildSubscriptionRql,
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";
import {rbcf} from "./rbcf";
import {CONTENT_TYPE_RBCF} from "./content-types";
import {toCamelCaseKeys, toSnakeCaseKeys, WIRE_PASSTHROUGH_KEYS} from "./case";

const enum BinaryKind {
    Response = 0x00,
    Change = 0x01,
    BatchChange = 0x02,
}

interface BinaryEnvelope {
    kind: BinaryKind;
    id: string;
    meta?: ResponseMeta;
    rbcf: Uint8Array;
}

interface BatchBinaryEnvelope {
    batchId: string;
    entries: Array<{ subscriptionId: string; rbcf: Uint8Array }>;
}

function decodeEnvelope(bytes: Uint8Array): BinaryEnvelope | null {
    if (bytes.length < 5) return null;
    const kind = bytes[0] as BinaryKind;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const idLen = view.getUint32(1, true);
    if (bytes.length < 5 + idLen + 4) return null;
    const decoder = new TextDecoder("utf-8");
    const id = decoder.decode(bytes.subarray(5, 5 + idLen));

    const metaLen = view.getUint32(5 + idLen, true);
    if (bytes.length < 5 + idLen + 4 + metaLen) return null;

    let meta: ResponseMeta | undefined;
    if (metaLen > 0) {
        const metaJson = decoder.decode(bytes.subarray(5 + idLen + 4, 5 + idLen + 4 + metaLen));
        try {
            meta = JSON.parse(metaJson);
        } catch (e) {
            console.error("Failed to parse RBCF metadata", e);
        }
    }

    const rbcfBytes = bytes.subarray(5 + idLen + 4 + metaLen);
    return {kind, id, meta, rbcf: rbcfBytes};
}

function decodeBatchEnvelope(bytes: Uint8Array): BatchBinaryEnvelope | null {
    if (bytes.length < 9) return null;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const decoder = new TextDecoder("utf-8");

    const batchIdLen = view.getUint32(1, true);
    let offset = 5;
    if (bytes.length < offset + batchIdLen + 4) return null;
    const batchId = decoder.decode(bytes.subarray(offset, offset + batchIdLen));
    offset += batchIdLen;

    const numEntries = view.getUint32(offset, true);
    offset += 4;

    const entries: Array<{ subscriptionId: string; rbcf: Uint8Array }> = [];
    for (let i = 0; i < numEntries; i++) {
        if (bytes.length < offset + 4) return null;
        const subIdLen = view.getUint32(offset, true);
        offset += 4;
        if (bytes.length < offset + subIdLen + 4) return null;
        const subscriptionId = decoder.decode(bytes.subarray(offset, offset + subIdLen));
        offset += subIdLen;

        const rbcfLen = view.getUint32(offset, true);
        offset += 4;
        if (bytes.length < offset + rbcfLen) return null;
        const rbcfBytes = bytes.subarray(offset, offset + rbcfLen);
        offset += rbcfLen;

        entries.push({subscriptionId, rbcf: rbcfBytes});
    }

    return {batchId, entries};
}

export interface WsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    maxReconnectAttempts?: number;
    reconnectDelayMs?: number;
    signal?: AbortSignal;
    /**
     * Wire format for data frames. Defaults to `"frames"`.
     *
     * - `"json"`   - rows-shape JSON: `[[{col: val, ...}, ...], ...]`
     * - `"frames"` - frames-shape JSON: columnar frames (default)
     * - `"rbcf"`   - frames-shape binary (RBCF)
     */
    format?: "json" | "frames" | "rbcf";
    /** Invoked when the connection drops, before any reconnection attempt. */
    onDisconnect?: () => void;
    /** Invoked after a successful reconnection, once all subscriptions are re-established. */
    onReconnect?: () => void;
}

interface SubscriptionState<T = any> {
    subscriptionId: string;
    rql: string;
    params?: any;
    shape?: ShapeNode;
    callbacks: SubscriptionCallbacks<T>;
    config?: SubscriptionConfig;
}

interface BatchState {
    batchId: string;
    members: BatchSubscriptionMember[];
    membersBySubId: Map<string, SubscriptionState>;
    batchCallbacks?: BatchSubscriptionCallbacks;
}

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | CallResponse | SubscribedResponse | UnsubscribedResponse | BatchSubscribedResponse | BatchUnsubscribedResponse | LogoutResponse;

async function createWebSocket(url: string): Promise<WebSocket> {
    let socket: WebSocket;
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        socket = new WebSocket(url);
    } else {
        //@ts-ignore
        const wsModule = await import("ws");
        socket = new wsModule.WebSocket(url);
    }
    try {
        (socket as any).binaryType = "arraybuffer";
    } catch {
    }
    return socket;
}

interface PendingEntry {
    type: string;
    handler: (response: ResponsePayload) => void;
}


export class WsClient {
    private options: WsClientOptions;
    private nextId: number;
    private socket: WebSocket;
    private pending = new Map<string, PendingEntry>();
    private reconnectAttempts: number = 0;
    private shouldReconnect: boolean = true;
    private isReconnecting: boolean = false;
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    private reconnectCancel: (() => void) | null = null;
    private subscriptions = new Map<string, SubscriptionState>();
    private batches = new Map<string, BatchState>();
    private subToBatch = new Map<string, string>();

    private constructor(socket: WebSocket, options: WsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.setupSocketHandlers();
    }

    static async connect(options: WsClientOptions): Promise<WsClient> {
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

        return new WsClient(socket, options);
    }

    /**
     * @param rql - RQL string to execute
     */
    async admin<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.adminWithMeta(rql, params, shapes);
        return frames;
    }

    async adminWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Admin", rql, params, shapes);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.commandWithMeta(rql, params, shapes);
        return frames;
    }

    async commandWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Command", rql, params, shapes);
    }


    /**
     * @param rql - RQL string to execute
     */
    async query<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.queryWithMeta(rql, params, shapes);
        return frames;
    }

    async queryWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute("Query", rql, params, shapes);
    }

    /**
     * @param name - globally-unique name of the WS binding to invoke
     */
    async call<const S extends readonly ShapeNode[]>(
        name: string,
        params: any,
        shapes: S
    ): Promise<FrameResults<S>> {
        const { frames } = await this.callWithMeta(name, params, shapes);
        return frames;
    }

    async callWithMeta<const S extends readonly ShapeNode[]>(
        name: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        const id = `req-${this.nextId++}`;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { result, meta } = await this.sendWithMeta({
            id,
            type: "Call",
            payload: {
                name,
                params: encodedParams
            },
        } as CallRequest);

        return { frames: transformFrames(result, shapes), meta };
    }

    private async execute<const S extends readonly ShapeNode[]>(
        type: "Admin" | "Command" | "Query",
        rql: string,
        params: any,
        shapes: S
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        const id = `req-${this.nextId++}`;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { result, meta } = await this.sendWithMeta({
            id,
            type,
            payload: {
                rql,
                params: encodedParams
            },
        } as AdminRequest | CommandRequest | QueryRequest);

        return { frames: transformFrames(result, shapes), meta };
    }

    async subscribe<T = any>(
        rql: string,
        params: any,
        shape: ShapeNode | undefined,
        callbacks: SubscriptionCallbacks<T>,
        config?: SubscriptionConfig
    ): Promise<string> {
        const id = `sub-${this.nextId++}`;

        const subFormat = this.options.format === "rbcf" ? "rbcf" : "frames";
        const wireRql = buildSubscriptionRql(rql, config);
        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;
        const request: SubscribeRequest = {
            id,
            type: "Subscribe",
            payload: {rql: wireRql, params: encodedParams, format: subFormat} as any
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "Subscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "Subscribed") {
                        const subscriptionId = response.payload.subscriptionId;

                        this.subscriptions.set(subscriptionId, {
                            subscriptionId,
                            rql,
                            params,
                            shape,
                            callbacks,
                            config
                        });

                        resolve(subscriptionId);
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    async unsubscribe(subscriptionId: string): Promise<void> {
        const id = `unsub-${this.nextId++}`;

        const request: UnsubscribeRequest = {
            id,
            type: "Unsubscribe",
            payload: {subscriptionId: subscriptionId}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "Unsubscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "Unsubscribed") {
                        this.subscriptions.delete(subscriptionId);
                        resolve();
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    async batchSubscribe(
        members: BatchSubscriptionMember[],
        batchCallbacks?: BatchSubscriptionCallbacks
    ): Promise<BatchSubscription> {
        if (members.length === 0) {
            throw new Error("batchSubscribe requires at least one member");
        }

        const id = `batch-sub-${this.nextId++}`;
        const subFormat = this.options.format === "rbcf" ? "rbcf" : "frames";
        const request: BatchSubscribeRequest = {
            id,
            type: "BatchSubscribe",
            payload: {
                queries: members.map(m => buildSubscriptionRql(m.rql, m.config)),
                format: subFormat as any
            }
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "BatchSubscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                        return;
                    }
                    if (response.type !== "BatchSubscribed") {
                        reject(new Error("Unexpected response type"));
                        return;
                    }

                    const {batchId, members: memberInfos} = response.payload;
                    const membersBySubId = new Map<string, SubscriptionState>();
                    const subscriptionIds: string[] = new Array(members.length);

                    for (const info of memberInfos) {
                        const member = members[info.index];
                        if (!member) continue;
                        subscriptionIds[info.index] = info.subscriptionId;
                        membersBySubId.set(info.subscriptionId, {
                            subscriptionId: info.subscriptionId,
                            rql: member.rql,
                            params: member.params,
                            shape: member.shape,
                            callbacks: member.callbacks,
                            config: member.config
                        });
                        this.subToBatch.set(info.subscriptionId, batchId);
                    }

                    this.batches.set(batchId, {
                        batchId,
                        members,
                        membersBySubId,
                        batchCallbacks
                    });

                    resolve({batchId, subscriptionIds});
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    async batchUnsubscribe(batchId: string): Promise<void> {
        const id = `batch-unsub-${this.nextId++}`;

        const request: BatchUnsubscribeRequest = {
            id,
            type: "BatchUnsubscribe",
            payload: {batchId}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "BatchUnsubscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "BatchUnsubscribed") {
                        this.cleanupBatch(batchId);
                        resolve();
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    private cleanupBatch(batchId: string): void {
        const batch = this.batches.get(batchId);
        if (!batch) return;
        for (const subId of batch.membersBySubId.keys()) {
            this.subToBatch.delete(subId);
        }
        this.batches.delete(batchId);
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest | CallRequest): Promise<any> {
        const { result } = await this.sendWithMeta(req);
        return result;
    }

    async sendWithMeta(
        req: AdminRequest | CommandRequest | QueryRequest | CallRequest,
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
            payload: { ...req.payload, format: this.wireFormat() },
        } as AdminRequest | CommandRequest | QueryRequest | CallRequest;

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
                }
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

        if (this.wireFormat() === "json") {
            return { result: response.payload.body ?? [], meta };
        }
        const frames = response.payload.body?.frames || [];
        return {
            result: frames.map((frame: any) => columnsToRows(frame.columns)),
            meta,
        };
    }

    private wireFormat(): "json" | "frames" | "rbcf" {
        return this.options.format ?? "frames";
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
                }
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
                }
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
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys({id, type: "Logout"}, WIRE_PASSTHROUGH_KEYS)));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        this.options = {...this.options, token: undefined};
    }

    async disconnect(): Promise<void> {
        this.shouldReconnect = false;
        this.subscriptions.clear();
        this.batches.clear();
        this.subToBatch.clear();

        if (this.reconnectTimer !== null) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.reconnectCancel !== null) {
            const cancel = this.reconnectCancel;
            this.reconnectCancel = null;
            cancel();
        }

        const socket = this.socket;
        if (socket.readyState === 3) {
            return;
        }

        await new Promise<void>(resolve => {
            const closeTimeoutMs = 250;
            let settled = false;
            const finish = () => {
                if (settled) return;
                settled = true;
                clearTimeout(timeout);
                socket.removeEventListener("close", onClose);
                resolve();
            };
            const onClose = () => finish();
            const timeout = setTimeout(finish, closeTimeoutMs);
            socket.addEventListener("close", onClose);
            try {
                socket.close();
            } catch {
                finish();
            }
        });
    }

    private handleDisconnect() {
        this.options.onDisconnect?.();
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

        const cancelled = await new Promise<boolean>(resolve => {
            this.reconnectCancel = () => {
                if (this.reconnectTimer !== null) {
                    clearTimeout(this.reconnectTimer);
                    this.reconnectTimer = null;
                }
                this.reconnectCancel = null;
                resolve(true);
            };
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this.reconnectCancel = null;
                resolve(false);
            }, delay);
        });

        if (cancelled || !this.shouldReconnect) {
            this.isReconnecting = false;
            return;
        }

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

            if (!this.shouldReconnect) {
                socket.close();
                this.isReconnecting = false;
                return;
            }

            if (this.options.token) {
                socket.send(JSON.stringify(toSnakeCaseKeys({id: "auth-1", type: "Auth", payload: {token: this.options.token}}, WIRE_PASSTHROUGH_KEYS)));
            }

            this.socket = socket;
            this.setupSocketHandlers();
            this.reconnectAttempts = 0;
            this.isReconnecting = false;

            await this.resubscribeAll();
            this.options.onReconnect?.();
        } catch (error) {
            this.isReconnecting = false;
            this.handleDisconnect();
        }
    }

    private async resubscribeAll(): Promise<void> {
        const subscriptionsToReestablish = Array.from(this.subscriptions.values());
        const batchesToReestablish = Array.from(this.batches.values());

        this.subscriptions.clear();
        this.batches.clear();
        this.subToBatch.clear();

        for (const state of subscriptionsToReestablish) {
            try {
                await (this.subscribe as any)(state.rql, state.params, state.shape, state.callbacks, state.config);
            } catch (err) {
                console.error(`Failed to resubscribe to ${state.rql}:`, err);
            }
        }

        for (const batch of batchesToReestablish) {
            try {
                await this.batchSubscribe(batch.members, batch.batchCallbacks);
            } catch (err) {
                console.error(`Failed to re-establish batch subscription:`, err);
            }
        }
    }

    private findSubscriptionState(subscriptionId: string): SubscriptionState | undefined {
        const single = this.subscriptions.get(subscriptionId);
        if (single) return single;
        const batchId = this.subToBatch.get(subscriptionId);
        if (!batchId) return undefined;
        return this.batches.get(batchId)?.membersBySubId.get(subscriptionId);
    }

    private handleChangeMessage(msg: ChangeMessage): void {
        const {subscriptionId, body} = msg.payload;
        const state = this.findSubscriptionState(subscriptionId);

        if (!state) {
            console.error('No state for subscriptionId:', subscriptionId);
            return;
        }

        const frames = body?.frames || [];
        for (const frame of frames) {
            this.dispatchChangeFrame(state, frame);
        }
    }

    private dispatchChangeFrame(state: SubscriptionState, frame: any): void {
        const rows = this.frameToRows(frame, state.shape);
        if (rows.length === 0) return;

        switch (frame.op) {
            case 2:
                state.callbacks.onUpdate?.(rows);
                break;
            case 3:
                state.callbacks.onRemove?.(rows);
                break;
            default:
                state.callbacks.onInsert?.(rows);
                break;
        }
    }

    private frameToRows(frame: any, shape?: ShapeNode): any[] {
        if (!frame.columns || frame.columns.length === 0) return [];

        const rowCount = frame.columns[0].payload.length;
        const rowNumbers = frame.row_numbers;
        const rows: any[] = [];

        for (let i = 0; i < rowCount; i++) {
            const row: any = {};
            for (const col of frame.columns) {
                row[col.name] = decode({type: col.type, value: col.payload[i]});
            }
            rows.push(row);
        }

        const shaped = shape ? rows.map(row => transformResult(row, shape)) : rows;

        if (rowNumbers) {
            for (let i = 0; i < shaped.length; i++) {
                if (rowNumbers[i] !== undefined) shaped[i][ROW_NUMBER_KEY] = Number(rowNumbers[i]);
            }
        }

        return shaped;
    }

    private handleBatchChange(msg: BatchChangeMessage): void {
        const {entries} = msg.payload;
        for (const entry of entries) {
            this.handleChangeMessage({
                type: "Change",
                payload: {
                    subscriptionId: entry.subscriptionId,
                    contentType: entry.contentType,
                    body: entry.body
                }
            });
        }
    }

    private handleBatchMemberClosed(msg: BatchMemberClosedMessage): void {
        const {batchId, subscriptionId} = msg.payload;
        const batch = this.batches.get(batchId);
        if (!batch) return;
        batch.membersBySubId.delete(subscriptionId);
        this.subToBatch.delete(subscriptionId);
        batch.batchCallbacks?.onMemberClosed?.(subscriptionId);
    }

    private handleBatchClosed(msg: BatchClosedMessage): void {
        const {batchId} = msg.payload;
        const batch = this.batches.get(batchId);
        if (!batch) return;
        this.cleanupBatch(batchId);
        batch.batchCallbacks?.onClosed?.();
    }

    private setupSocketHandlers() {
        this.socket.onmessage = (event) => {
            const data = event.data;

            if (data instanceof ArrayBuffer) {
                this.handleBinaryMessage(new Uint8Array(data));
                return;
            }
            if (typeof data !== "string") {
                const buf = data as { buffer?: ArrayBuffer; byteOffset?: number; byteLength?: number };
                if (buf && typeof buf.byteLength === "number" && buf.buffer instanceof ArrayBuffer) {
                    const u8 = new Uint8Array(buf.buffer, buf.byteOffset ?? 0, buf.byteLength);
                    this.handleBinaryMessage(u8);
                    return;
                }
                return;
            }

            const msg = toCamelCaseKeys<any>(JSON.parse(data), WIRE_PASSTHROUGH_KEYS);

            if (!msg.id) {
                switch (msg.type) {
                    case "Change":
                        this.handleChangeMessage(msg);
                        return;
                    case "BatchChange":
                        this.handleBatchChange(msg);
                        return;
                    case "BatchMemberClosed":
                        this.handleBatchMemberClosed(msg);
                        return;
                    case "BatchClosed":
                        this.handleBatchClosed(msg);
                        return;
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

    private handleBinaryMessage(bytes: Uint8Array) {
        if (bytes.length > 0 && bytes[0] === BinaryKind.BatchChange) {
            this.handleBinaryBatchMessage(bytes);
            return;
        }

        const envelope = decodeEnvelope(bytes);
        if (!envelope) return;
        const {kind, id, rbcf: rbcfBytes} = envelope;

        let frames: any[];
        try {
            frames = rbcf.decode(rbcfBytes);
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
            entry.handler({
                id,
                type: entry.type,
                payload: {
                    contentType: CONTENT_TYPE_RBCF,
                    body: { frames },
                    meta: envelope.meta,
                },
            } as ResponsePayload);
            return;
        }

        if (kind === BinaryKind.Change) {
            this.handleChangeMessage({
                type: "Change",
                payload: {
                    subscriptionId: id,
                    contentType: CONTENT_TYPE_RBCF,
                    body: { frames },
                }
            });
        }
    }

    private handleBinaryBatchMessage(bytes: Uint8Array) {
        const envelope = decodeBatchEnvelope(bytes);
        if (!envelope) return;

        const batch = this.batches.get(envelope.batchId);

        for (const entry of envelope.entries) {
            let frames: any[];
            try {
                frames = rbcf.decode(entry.rbcf);
            } catch (e) {
                const err = e instanceof Error ? e : new Error(String(e));
                if (batch?.batchCallbacks?.onEntryError) {
                    batch.batchCallbacks.onEntryError(entry.subscriptionId, err);
                } else {
                    console.error(`Failed to decode RBCF batch entry for ${entry.subscriptionId}: ${err.message}`);
                }
                continue;
            }
            this.handleChangeMessage({
                type: "Change",
                payload: {
                    subscriptionId: entry.subscriptionId,
                    contentType: CONTENT_TYPE_RBCF,
                    body: { frames },
                }
            });
        }
    }
}
