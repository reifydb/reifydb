// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
import { ReifyError } from "@reifydb/core";

// Re-export types that are actually available in flow
export type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
export { ReifyError } from "@reifydb/core";

export interface AdminRequest {
    id: string;
    type: "Admin";
    payload: {
        statements: string[];
        params?: Params;
        format?: string;
        unwrap?: boolean;
    }
}

export interface AdminResponse {
    id: string;
    type: "Admin";
    payload: {
        content_type: string;
        body: any;
    };
}

export interface CommandRequest {
    id: string;
    type: "Command";
    payload: {
        statements: string[];
        params?: Params;
        format?: string;
        unwrap?: boolean;
    }
}

export interface CommandResponse {
    id: string;
    type: "Command";
    payload: {
        content_type: string;
        body: any;
    };
}

export interface QueryRequest {
    id: string;
    type: "Query";
    payload: {
        statements: string[];
        params?: Params;
        format?: string;
        unwrap?: boolean;
    }
}

export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        content_type: string;
        body: any;
    };
}

export interface SubscribeRequest {
    id: string;
    type: "Subscribe";
    payload: {
        query: string;
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
    onInsert?: (rows: T[]) => void;
    onUpdate?: (rows: T[]) => void;
    onRemove?: (rows: T[]) => void;
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
        status?: string;
        token?: string;
        identity?: string;
    };
}

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