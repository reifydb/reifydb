// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {WsClient, WsClientOptions} from "./ws";
import {JsonWsClient, JsonWsClientOptions} from "./json";
import {JsonHttpClient, JsonHttpClientOptions} from "./http";

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
     * Connect to ReifyDB via WebSocket with JSON format responses
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected JSON WebSocket client
     */
    static async connect_json_ws(url: string, options: Omit<JsonWsClientOptions, 'url'> = {}): Promise<JsonWsClient> {
        return JsonWsClient.connect({url, ...options});
    }

    /**
     * Connect to ReifyDB via HTTP with JSON format responses
     * @param url Base HTTP URL
     * @param options Optional configuration
     * @returns JSON HTTP client
     */
    static connect_json_http(url: string, options: Omit<JsonHttpClientOptions, 'url'> = {}): JsonHttpClient {
        return JsonHttpClient.connect({url, ...options});
    }
}

export {ReifyError, asFrameResults} from "@reifydb/core";
export type {FrameResults, SingleFrameResult, Diagnostic, Fragment, DiagnosticColumn} from "@reifydb/core";
export {WsClient} from "./ws";
export type {WsClientOptions} from "./ws";
export {JsonWsClient} from "./json";
export type {JsonWsClientOptions} from "./json";
export {JsonHttpClient} from "./http";
export type {JsonHttpClientOptions} from "./http";
export type {
    SubscribeRequest,
    SubscribedResponse,
    UnsubscribeRequest,
    UnsubscribedResponse,
    ChangeMessage,
    SubscriptionCallbacks,
    SubscriptionOperation
} from './types';