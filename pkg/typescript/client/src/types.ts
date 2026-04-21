// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type { Params, Frame, Column, ErrorResponse, ShapeNode } from "@reifydb/core";
import { ReifyError } from "@reifydb/core";

// Re-export types that are actually available in flow
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
        content_type: string;
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
        content_type: string;
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
        content_type: string;
        body: any;
        meta?: ResponseMeta;
    };
}

export interface SubscribeRequest {
    id: string;
    type: "Subscribe";
    payload: {
        rql: string;
        format?: "json" | "rbcf";
    };
}

export interface SubscribedResponse {
    id: string;
    type: "Subscribed";
    payload: {
        subscription_id: string;
    };
}

export interface UnsubscribeRequest {
    id: string;
    type: "Unsubscribe";
    payload: {
        subscription_id: string;
    };
}

export interface UnsubscribedResponse {
    id: string;
    type: "Unsubscribed";
    payload: {
        subscription_id: string;
    };
}

export interface ChangeMessage {
    // No id field - server-initiated
    type: "Change";
    payload: {
        subscription_id: string;
        content_type: string;
        body: any;
    };
}

export type SubscriptionOperation = 'INSERT' | 'UPDATE' | 'REMOVE';

export interface SubscriptionCallbacks<T = any> {
    on_insert?: (rows: T[]) => void;
    on_update?: (rows: T[]) => void;
    on_remove?: (rows: T[]) => void;
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
    subscription_id: string;
}

export interface BatchSubscribedResponse {
    id: string;
    type: "BatchSubscribed";
    payload: {
        batch_id: string;
        members: BatchMemberInfo[];
    };
}

export interface BatchUnsubscribeRequest {
    id: string;
    type: "BatchUnsubscribe";
    payload: {
        batch_id: string;
    };
}

export interface BatchUnsubscribedResponse {
    id: string;
    type: "BatchUnsubscribed";
    payload: {
        batch_id: string;
    };
}

export interface BatchChangeMessage {
    type: "BatchChange";
    payload: {
        batch_id: string;
        entries: Array<{
            subscription_id: string;
            content_type: string;
            body: any;
        }>;
    };
}

export interface BatchMemberClosedMessage {
    type: "BatchMemberClosed";
    payload: {
        batch_id: string;
        subscription_id: string;
    };
}

export interface BatchClosedMessage {
    type: "BatchClosed";
    payload: {
        batch_id: string;
    };
}

export interface BatchSubscriptionMember<T = any> {
    rql: string;
    params?: any;
    shape?: ShapeNode;
    callbacks: SubscriptionCallbacks<T>;
}

export interface BatchSubscriptionCallbacks {
    on_member_closed?: (subscription_id: string) => void;
    on_closed?: () => void;
    on_entry_error?: (subscription_id: string, error: Error) => void;
}

export interface BatchSubscription {
    batch_id: string;
    subscription_ids: string[];
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
        challenge_id?: string;
        payload?: { message: string; nonce: string };
        reason?: string;
    };
}

export type LoginChallengeResult =
    | { kind: "authenticated"; token: string; identity: string }
    | { kind: "challenge"; challenge_id: string; message: string; nonce: string };

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