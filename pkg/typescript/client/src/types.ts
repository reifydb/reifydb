// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
import { ReifyError } from "@reifydb/core";

// Re-export types that are actually available in flow
export type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
export { ReifyError } from "@reifydb/core";

export interface CommandRequest {
    id: string;
    type: "Command";
    payload: {
        statements: string[];
        params?: Params;
    }
}

export interface CommandResponse {
    id: string;
    type: "Command";
    payload: {
        frames: Frame[];
    };
}

export interface QueryRequest {
    id: string;
    type: "Query";
    payload: {
        statements: string[];
        params?: Params;
    }
}

export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        frames: Frame[];
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
        frame: Frame;
    };
}

export type SubscriptionOperation = 'INSERT' | 'UPDATE' | 'REMOVE';

export interface SubscriptionCallbacks<T = any> {
    onInsert?: (rows: T[]) => void;
    onUpdate?: (rows: T[]) => void;
    onRemove?: (rows: T[]) => void;
}