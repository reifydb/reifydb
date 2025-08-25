/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {WsClient, WsClientOptions} from "./ws";

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
}

export {ReifyError, asFrameResults} from "@reifydb/core";
export type {FrameResults, SingleFrameResult, Diagnostic, Fragment, DiagnosticColumn} from "@reifydb/core";
export {WsClient} from "./ws";
export type {WsClientOptions} from "./ws";