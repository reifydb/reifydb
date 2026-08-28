// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type { Params, Frame, Column, ErrorResponse, ShapeNode, DurationValue } from "@reifydb/core";
import { ReifyError } from "@reifydb/core";

export type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
export { ReifyError } from "@reifydb/core";

export interface AdminRequest {
    id: string;
    type: "Admin";
    payload: {
        rql: string;
        params?: Params;
        format?: "json" | "rbcf";
        unwrap?: boolean;
    }
}

export interface ResponseMeta {
    fingerprint: string;
    duration: string;
}

export interface AdminResponse {
    id: string;
    type: "Admin";
    payload: {
        contentType: string;
        body: any;
        meta?: ResponseMeta;
    };
}

export interface CommandRequest {
    id: string;
    type: "Command";
    payload: {
        rql: string;
        params?: Params;
        format?: "json" | "rbcf";
        unwrap?: boolean;
    }
}

export interface CommandResponse {
    id: string;
    type: "Command";
    payload: {
        contentType: string;
        body: any;
        meta?: ResponseMeta;
    };
}

export interface QueryRequest {
    id: string;
    type: "Query";
    payload: {
        rql: string;
        params?: Params;
        format?: "json" | "rbcf";
        unwrap?: boolean;
    }
}

export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        contentType: string;
        body: any;
        meta?: ResponseMeta;
    };
}

export interface CallRequest {
    id: string;
    type: "Call";
    payload: {
        name: string;
        params?: Params;
        format?: "json" | "rbcf";
    }
}

export interface CallResponse {
    id: string;
    type: "Call";
    payload: {
        contentType: string;
        body: any;
        meta?: ResponseMeta;
    };
}

export interface SubscribeRequest {
    id: string;
    type: "Subscribe";
    payload: {
        rql: string;
        params?: Params;
        format?: "json" | "rbcf";
    };
}

export interface SubscribedResponse {
    id: string;
    type: "Subscribed";
    payload: {
        subscriptionId: string;
    };
}

export interface UnsubscribeRequest {
    id: string;
    type: "Unsubscribe";
    payload: {
        subscriptionId: string;
    };
}

export interface UnsubscribedResponse {
    id: string;
    type: "Unsubscribed";
    payload: {
        subscriptionId: string;
    };
}

export interface ChangeMessage {
    type: "Change";
    payload: {
        subscriptionId: string;
        contentType: string;
        body: any;
    };
}

export type SubscriptionOperation = 'INSERT' | 'UPDATE' | 'REMOVE';

/**
 * Every subscription row carries the server's identity for the underlying row under `#rownum`.
 * `#` is not a legal character in a user column name, so this key can never be shadowed.
 * Use it to key client-side state; `id` is a user column and may not exist or may not be unique.
 */
export type SubscriptionRow<T> = T & { "#rownum": number };

export interface SubscriptionCallbacks<T = any> {
    onInsert?: (rows: SubscriptionRow<T>[]) => void;
    onUpdate?: (rows: SubscriptionRow<T>[]) => void;
    onRemove?: (rows: SubscriptionRow<T>[]) => void;
}

export interface HydrationConfig {
    enabled: boolean;
    maxRows?: number;
}

export interface SubscriptionConfig {
    hydration?: HydrationConfig;
    throttle?: DurationValue;
    linger?: DurationValue;
}

export function defaultHydrationConfig(): HydrationConfig {
    return { enabled: true };
}

export function defaultSubscriptionConfig(): SubscriptionConfig {
    return { hydration: defaultHydrationConfig() };
}

function durationLiteral(knob: string, value: DurationValue): string {
    if (value.isNegative()) {
        throw new Error(`${knob} must not be negative`);
    }
    const literal = value.toString();
    if (literal === 'none') {
        throw new Error(`${knob} must be a duration`);
    }
    return literal;
}

export function buildSubscriptionRql(body: string, config?: SubscriptionConfig): string {
    const h = config?.hydration ?? defaultHydrationConfig();
    const enabled = h.enabled;
    let opts = h.maxRows !== undefined
        ? `hydration: { enabled: ${enabled}, max_rows: ${h.maxRows} }`
        : `hydration: { enabled: ${enabled} }`;
    if (config?.throttle !== undefined) {
        opts += `, throttle: ${durationLiteral('throttle', config.throttle)}`;
    }
    if (config?.linger !== undefined) {
        opts += `, linger: ${durationLiteral('linger', config.linger)}`;
    }
    return `CREATE SUBSCRIPTION WITH { ${opts} } AS { ${body} }`;
}

export interface BatchSubscribeRequest {
    id: string;
    type: "BatchSubscribe";
    payload: {
        queries: string[];
        format?: "json" | "frames" | "rbcf";
    };
}

export interface BatchMemberInfo {
    index: number;
    subscriptionId: string;
}

export interface BatchSubscribedResponse {
    id: string;
    type: "BatchSubscribed";
    payload: {
        batchId: string;
        members: BatchMemberInfo[];
    };
}

export interface BatchUnsubscribeRequest {
    id: string;
    type: "BatchUnsubscribe";
    payload: {
        batchId: string;
    };
}

export interface BatchUnsubscribedResponse {
    id: string;
    type: "BatchUnsubscribed";
    payload: {
        batchId: string;
    };
}

export interface BatchChangeMessage {
    type: "BatchChange";
    payload: {
        batchId: string;
        entries: Array<{
            subscriptionId: string;
            contentType: string;
            body: any;
        }>;
    };
}

export interface BatchMemberClosedMessage {
    type: "BatchMemberClosed";
    payload: {
        batchId: string;
        subscriptionId: string;
    };
}

export interface BatchClosedMessage {
    type: "BatchClosed";
    payload: {
        batchId: string;
    };
}

export interface BatchSubscriptionMember<T = any> {
    rql: string;
    params?: any;
    shape?: ShapeNode;
    callbacks: SubscriptionCallbacks<T>;
    config?: SubscriptionConfig;
}

export interface BatchSubscriptionCallbacks {
    onMemberClosed?: (subscriptionId: string) => void;
    onClosed?: () => void;
    onEntryError?: (subscriptionId: string, error: Error) => void;
}

export interface BatchSubscription {
    batchId: string;
    subscriptionIds: string[];
}

export interface AuthRequest {
    id: string;
    type: "Auth";
    payload: {
        token?: string;
        method?: string;
        credentials?: Record<string, string>;
    };
}

export interface AuthResponse {
    id: string;
    type: "Auth";
    payload: {
        status?: "authenticated" | "challenge" | "failed";
        token?: string;
        identity?: string;
        challengeId?: string;
        payload?: { message: string; nonce: string };
        reason?: string;
    };
}

export type LoginChallengeResult =
    | { kind: "authenticated"; token: string; identity: string }
    | { kind: "challenge"; challengeId: string; message: string; nonce: string };

export interface LogoutRequest {
    id: string;
    type: "Logout";
    payload: {};
}

export interface LogoutResponse {
    id: string;
    type: "Logout";
    payload: {
        status: string;
    };
}

export interface LoginResult {
    token: string;
    identity: string;
}
