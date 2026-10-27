// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    columnsToRows,
    framesFromWire,
    transformFrames,
    checkFrames
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
    SubscribeItem,
    SubscriptionClosedMessage,
    SubscriptionInfo
} from "./types";
import {
    encodeSubscribeOptions,
    ReifyError
} from "./types";
import {jsonResponseToRows} from "./json-decode";
import {encodeParams} from "./encoder";
import {rbcf} from "./rbcf";
import {CONTENT_TYPE_RBCF} from "./content-types";
import {toCamelCaseKeys, toSnakeCaseKeys, WIRE_PASSTHROUGH_KEYS} from "./case";
import {BinaryKind, decodeChangeListEnvelope, decodeEnvelope, dispatchChange, envelopeIdOf, reportSubscriptionError} from "./subscription-decode";

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

interface SubscriptionState<T = any> extends SubscribeItem<T> {
    subscriptionId: string;
    group: SubscriptionState[];
}

interface Caller<T> {
    resolve: (value: T) => void;
    reject: (error: unknown) => void;
}

interface SubscribeIntent {
    subscriptions: SubscribeItem[];
    caller?: Caller<string[]>;
}

const CONNECTION_LOST: ErrorResponse = {
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

type ResponsePayload = ErrorResponse | AdminResponse | AuthResponse | CommandResponse | QueryResponse | CallResponse | SubscribedResponse | UnsubscribedResponse | LogoutResponse;

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
    onConnectionLost?: () => void;
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
    private resubscribing = new Map<string, SubscriptionState>();
    private waiting: SubscribeIntent[] = [];

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
        } as CallRequest, shapes);

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
        } as AdminRequest | CommandRequest | QueryRequest, shapes);

        return { frames: transformFrames(result, shapes), meta };
    }

    async subscribe(subscriptions: SubscribeItem[]): Promise<string[]> {
        if (subscriptions.length === 0) {
            throw new Error("subscribe requires at least one subscription");
        }
        return new Promise((resolve, reject) => {
            this.dispatch({subscriptions, caller: {resolve, reject}});
        });
    }

    private dispatch(intent: SubscribeIntent): void {
        if (this.socket.readyState === 1) {
            this.sendIntent(intent);
        } else if (this.shouldReconnect) {
            this.waiting.push(intent);
        } else {
            this.failIntent(intent, new ReifyError(CONNECTION_LOST));
        }
    }

    private sendIntent(intent: SubscribeIntent): Promise<void> {
        return new Promise(done => {
            let request: SubscribeRequest;
            try {
                request = this.subscribeRequest(intent);
            } catch (error) {
                this.failIntent(intent, error);
                done();
                return;
            }
            this.pending.set(request.id, {
                type: request.type,
                handler: (response) => {
                    this.settleIntent(intent, response);
                    done();
                },
                onConnectionLost: () => {
                    this.waiting.push(intent);
                    done();
                }
            });
            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    private subscribeRequest(intent: SubscribeIntent): SubscribeRequest {
        const format = this.options.format === "rbcf" ? "rbcf" : "frames";
        return {
            id: `sub-${this.nextId++}`,
            type: "Subscribe",
            payload: {
                subscriptions: intent.subscriptions.map(m => ({
                    rql: m.rql,
                    params: m.params !== undefined && m.params !== null ? encodeParams(m.params) : undefined,
                    options: encodeSubscribeOptions(m.config)
                })) as any,
                format
            }
        };
    }

    private settleIntent(intent: SubscribeIntent, response: ResponsePayload): void {
        if (response.type === "Err") {
            this.failIntent(intent, new ReifyError(response));
        } else if (response.type === "Subscribed") {
            this.attach(intent, response.payload.subscriptions);
        } else {
            this.failIntent(intent, new Error("Unexpected response type"));
        }
    }

    private attach(intent: SubscribeIntent, acked: SubscriptionInfo[]): void {
        const group: SubscriptionState[] = [];
        const subscriptionIds: string[] = new Array(intent.subscriptions.length);
        for (const info of acked) {
            const subscription = intent.subscriptions[info.index];
            if (!subscription) continue;
            subscriptionIds[info.index] = info.subscriptionId;
            const state: SubscriptionState = {
                subscriptionId: info.subscriptionId,
                rql: subscription.rql,
                params: subscription.params,
                shape: subscription.shape,
                callbacks: subscription.callbacks,
                config: subscription.config,
                group
            };
            group.push(state);
            this.subscriptions.set(info.subscriptionId, state);
        }
        if (intent.caller) {
            intent.caller.resolve(subscriptionIds);
            return;
        }
        const released: SubscriptionState[] = [];
        intent.subscriptions.forEach((subscription, index) => {
            const id = subscriptionIds[index];
            const state = id === undefined ? undefined : this.subscriptions.get(id);
            if (!this.retire(subscription)) {
                if (state) released.push(state);
            } else if (state) {
                this.notifyResubscribed(state);
            } else {
                reportSubscriptionError(subscription, new Error("the subscribe ack named no id for this subscription"));
            }
        });
        this.giveBack(released);
    }

    private notifyResubscribed(state: SubscriptionState): void {
        try {
            state.callbacks.onResubscribe?.(state.subscriptionId);
        } catch (error) {
            reportSubscriptionError(state, error);
        }
    }

    private failIntent(intent: SubscribeIntent, error: unknown): void {
        if (intent.caller) {
            intent.caller.reject(error);
            return;
        }
        intent.subscriptions.filter(subscription => this.retire(subscription)).forEach(subscription => reportSubscriptionError(subscription, error));
    }

    private held(subscription: SubscribeItem): boolean {
        return this.resubscribing.get((subscription as SubscriptionState).subscriptionId) === subscription;
    }

    private retire(subscription: SubscribeItem): boolean {
        return this.held(subscription) && this.resubscribing.delete((subscription as SubscriptionState).subscriptionId);
    }

    private stillWanted(intent: SubscribeIntent): SubscribeIntent | undefined {
        if (intent.caller) {
            return intent;
        }
        const subscriptions = intent.subscriptions.filter(subscription => this.held(subscription));
        return subscriptions.length > 0 ? {...intent, subscriptions} : undefined;
    }

    private giveBack(states: SubscriptionState[]): void {
        if (states.length === 0) return;
        this.unsubscribe(states.map(state => state.subscriptionId))
            .catch(error => states.forEach(state => reportSubscriptionError(state, error)));
    }

    private forget(subscriptionId: string): void {
        this.subscriptions.delete(subscriptionId);
    }

    async unsubscribe(subscriptionIds: string[]): Promise<void> {
        const held = subscriptionIds.filter(id => this.subscriptions.has(id));
        subscriptionIds.filter(id => !this.subscriptions.has(id)).forEach(id => this.resubscribing.delete(id));
        if (held.length === 0) return;
        if (this.socket.readyState !== 1) {
            held.forEach(id => this.forget(id));
            return;
        }

        const id = `unsub-${this.nextId++}`;

        const request: UnsubscribeRequest = {
            id,
            type: "Unsubscribe",
            payload: {subscriptionIds: held}
        };

        return new Promise((resolve, reject) => {
            this.pending.set(id, {
                type: "Unsubscribe",
                handler: (response) => {
                    if (response.type === "Err") {
                        reject(new ReifyError(response));
                    } else if (response.type === "Unsubscribed") {
                        held.forEach(subscriptionId => this.forget(subscriptionId));
                        resolve();
                    } else {
                        reject(new Error("Unexpected response type"));
                    }
                },
                onConnectionLost: () => {
                    held.forEach(subscriptionId => this.forget(subscriptionId));
                    resolve();
                }
            });

            this.socket.send(JSON.stringify(toSnakeCaseKeys(request, WIRE_PASSTHROUGH_KEYS)));
        });
    }

    async send(req: AdminRequest | CommandRequest | QueryRequest | CallRequest): Promise<any> {
        const { result } = await this.sendWithMeta(req);
        return result;
    }

    async sendWithMeta(
        req: AdminRequest | CommandRequest | QueryRequest | CallRequest,
        shapes?: readonly ShapeNode[],
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
            return { result: jsonResponseToRows(response.payload.body ?? [], shapes), meta };
        }
        // An rbcf response was decoded from bytes and already carries the client's own types; a frames
        // response carries the wire's rendering and has to be read into them first.
        const raw = response.payload.body?.frames;
        if (!Array.isArray(raw)) {
            throw new Error(`A frames response must carry a list of frames, got ${JSON.stringify(response.payload.body)}`);
        }
        const contentType = (response.payload as any).contentType;
        const frames = contentType === CONTENT_TYPE_RBCF ? raw : framesFromWire(raw);
        if (shapes) checkFrames(frames, shapes);
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
        this.resubscribing.clear();
        this.failWaiting();

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

        if (this.isReconnecting) {
            return;
        }

        const maxAttempts = this.options.maxReconnectAttempts ?? 5;
        if (this.shouldReconnect && this.reconnectAttempts >= maxAttempts) {
            console.error(`Max reconnection attempts (${maxAttempts}) reached`);
            this.shouldReconnect = false;
        }

        if (!this.shouldReconnect) {
            this.failWaiting();
            return;
        }

        this.attemptReconnect();
    }

    private failWaiting(): void {
        const waiting = this.waiting;
        this.waiting = [];
        for (const intent of waiting) {
            intent.caller?.reject(new ReifyError(CONNECTION_LOST));
        }
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

            if (await this.resubscribeAll()) {
                this.options.onReconnect?.();
            }
        } catch (error) {
            this.isReconnecting = false;
            this.handleDisconnect();
        }
    }

    private async resubscribeAll(): Promise<boolean> {
        const socket = this.socket;
        const groups = new Set<SubscriptionState[]>();
        this.subscriptions.forEach(state => groups.add(state.group));
        const intents: SubscribeIntent[] = [
            ...Array.from(groups, (group): SubscribeIntent => ({
                subscriptions: group.filter(state => this.subscriptions.get(state.subscriptionId) === state)
            })),
            ...this.waiting
        ];

        this.subscriptions.forEach(state => this.resubscribing.set(state.subscriptionId, state));
        this.subscriptions.clear();
        this.waiting = [];

        for (let i = 0; i < intents.length; i++) {
            if (this.socket !== socket || socket.readyState !== 1) {
                this.waiting.push(...intents.slice(i));
                return false;
            }
            const intent = this.stillWanted(intents[i]);
            if (intent !== undefined) {
                await this.sendIntent(intent);
            }
        }
        return this.socket === socket && socket.readyState === 1;
    }

    private handleChangeMessage(msg: ChangeMessage): void {
        for (const entry of msg.payload.entries) {
            this.deliverChange(entry.subscriptionId, entry.contentType, entry.body);
        }
    }

    private deliverChange(subscriptionId: string, contentType: string, body: any): void {
        const state = this.subscriptions.get(subscriptionId);
        if (!state) {
            console.error('No state for subscriptionId:', subscriptionId);
            return;
        }
        dispatchChange(state, contentType, body);
    }

    private handleSubscriptionClosed(msg: SubscriptionClosedMessage): void {
        const state = this.subscriptions.get(msg.payload.subscriptionId);
        if (!state) return;
        this.forget(state.subscriptionId);
        try {
            state.callbacks.onClosed?.();
        } catch (error) {
            reportSubscriptionError(state, error);
        }
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
                    case "SubscriptionClosed":
                        this.handleSubscriptionClosed(msg);
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
        for (const entry of this.pending.values()) {
            if (entry.onConnectionLost) {
                entry.onConnectionLost();
            } else {
                entry.handler(CONNECTION_LOST);
            }
        }
        this.pending.clear();
    }

    private failBinaryEnvelope(bytes: Uint8Array, error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        const id = envelopeIdOf(bytes);
        const entry = id === undefined ? undefined : this.pending.get(id);
        if (entry === undefined) {
            console.error(`Failed to decode a binary envelope: ${message}`);
            return;
        }
        this.pending.delete(id!);
        entry.handler({
            id: id!,
            type: "Err",
            payload: {
                diagnostic: { code: "RBCF_DECODE", message, notes: [] }
            }
        } as ErrorResponse);
    }

    private handleBinaryMessage(bytes: Uint8Array) {
        if (bytes.length > 0 && bytes[0] === BinaryKind.Change) {
            this.handleBinaryChangeList(bytes);
            return;
        }

        let envelope;
        try {
            envelope = decodeEnvelope(bytes);
        } catch (e) {
            this.failBinaryEnvelope(bytes, e);
            return;
        }
        const {id, rbcf: rbcfBytes} = envelope;

        const entry = this.pending.get(id);
        if (!entry) return;
        this.pending.delete(id);

        let frames: any[];
        try {
            frames = rbcf.decode(rbcfBytes);
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            entry.handler({
                id,
                type: "Err",
                payload: {
                    diagnostic: { code: "RBCF_DECODE", message: msg, notes: [] }
                }
            } as ErrorResponse);
            return;
        }

        entry.handler({
            id,
            type: entry.type,
            payload: {
                contentType: CONTENT_TYPE_RBCF,
                body: { frames },
                meta: envelope.meta,
            },
        } as ResponsePayload);
    }

    private handleBinaryChangeList(bytes: Uint8Array) {
        let envelope;
        try {
            envelope = decodeChangeListEnvelope(bytes);
        } catch (e) {
            console.error(`Failed to decode a binary change envelope: ${e instanceof Error ? e.message : String(e)}`);
            return;
        }
        for (const entry of envelope.entries) {
            const state = this.subscriptions.get(entry.subscriptionId);
            if (!state) {
                console.error('No state for subscriptionId:', entry.subscriptionId);
                continue;
            }
            let frames: any[];
            try {
                frames = rbcf.decode(entry.rbcf);
            } catch (e) {
                reportSubscriptionError(state, e);
                continue;
            }
            dispatchChange(state, CONTENT_TYPE_RBCF, {frames});
        }
    }
}
