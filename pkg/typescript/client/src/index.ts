// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {WsClient, WsClientOptions} from "./ws";
import {HttpClient, HttpClientOptions} from "./http";

export class Client {
    /**
     * Connect to ReifyDB via WebSocket
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected WebSocket client
     */
    static async connect_ws(url: string, options: Omit<WsClientOptions, 'url'> = {}): Promise<WsClient> {
        return WsClient.connect({url, ...options});
    }

    /**
     * Connect to ReifyDB via HTTP
     * @param url HTTP URL
     * @param options Optional configuration
     * @returns HTTP client (sync, no connection to await)
     */
    static connect_http(url: string, options: Omit<HttpClientOptions, 'url'> = {}): HttpClient {
        return HttpClient.connect({url, ...options});
    }
}

export {ReifyError, asFrameResults} from "@reifydb/core";
export type {FrameResults, SingleFrameResult, Diagnostic, Fragment, DiagnosticColumn} from "@reifydb/core";
export {WsClient} from "./ws";
export type {WsClientOptions} from "./ws";
export {HttpClient} from "./http";
export type {HttpClientOptions} from "./http";
export type {
    SubscribeRequest,
    SubscribedResponse,
    UnsubscribeRequest,
    UnsubscribedResponse,
    ChangeMessage,
    SubscriptionCallbacks,
    SubscriptionOperation
} from './types';
