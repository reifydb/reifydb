// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {Client, WsClient, HttpClient, JsonHttpClient, JsonWebsocketClient, type WsClientOptions} from '@reifydb/client';

interface ConnectionState {
    client: WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient | null;
    is_connected: boolean;
    is_connecting: boolean;
    connection_error: string | null;
    listeners: Set<(state: ConnectionState) => void>;
}

export interface ConnectionConfig {
    url?: string;
    token?: string;
    format?: 'json';
    options?: Omit<WsClientOptions, 'url' | 'token'>;
}

export const DEFAULT_CONFIG: ConnectionConfig = {
    url: 'ws://127.0.0.1:8090',
    options: {timeout_ms: 30_000},
};

export const DEFAULT_URL = 'ws://127.0.0.1:8090';

export class Connection {
    private state: ConnectionState = {
        client: null,
        is_connected: false,
        is_connecting: false,
        connection_error: null,
        listeners: new Set(),
    };
    private config: ConnectionConfig;
    private connect_promise: Promise<void> | null = null;

    constructor(config: ConnectionConfig = DEFAULT_CONFIG) {
        this.config = {...DEFAULT_CONFIG, ...config};
    }

    set_config(config: ConnectionConfig): void {
        this.config = config;
    }

    get_config(): ConnectionConfig {
        return this.config;
    }

    async connect(url?: string, options?: Omit<WsClientOptions, 'url'>): Promise<void> {
        if (this.state.is_connected) {
            return;
        }

        // If already connecting, wait for the in-flight connection
        if (this.connect_promise) {
            return this.connect_promise;
        }

        const connect_url = url || this.config.url || DEFAULT_CONFIG.url!;
        const connect_options = {token: this.config.token, ...this.config.options, ...options};

        this.update_state({
            is_connecting: true,
            connection_error: null,
        });

        this.connect_promise = (async () => {
            try {
                const isHttp = connect_url.startsWith('http://') || connect_url.startsWith('https://');
                const isJson = this.config.format === 'json';
                let client: WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient;
                if (isHttp) {
                    client = isJson
                        ? Client.connect_json_http(connect_url, connect_options)
                        : Client.connect_http(connect_url, connect_options);
                } else {
                    client = isJson
                        ? await Client.connect_json_ws(connect_url, connect_options)
                        : await Client.connect_ws(connect_url, connect_options);
                }
                this.update_state({
                    client,
                    is_connected: true,
                    is_connecting: false,
                    connection_error: null,
                });
            } catch (err) {
                const error_message = err instanceof Error ? err.message : 'Failed to connect to ReifyDB';
                console.error('[Connection] Connection failed:', error_message, err);
                this.update_state({
                    client: null,
                    is_connected: false,
                    is_connecting: false,
                    connection_error: error_message,
                });
                throw err;
            } finally {
                this.connect_promise = null;
            }
        })();

        return this.connect_promise;
    }

    async disconnect(): Promise<void> {
        this.connect_promise = null;
        if (this.state.client) {
            try {
                if ('disconnect' in this.state.client) {
                    (this.state.client as WsClient).disconnect();
                    // Small delay to ensure WebSocket closes cleanly
                    await new Promise(resolve => setTimeout(resolve, 10));
                }
            } catch (err) {
                console.error('Error disconnecting:', err);
            }
        }

        this.update_state({
            client: null,
            is_connected: false,
            is_connecting: false,
            connection_error: null,
        });
    }

    async reconnect(url?: string, options?: Omit<WsClientOptions, 'url'>): Promise<void> {
        await this.disconnect();
        await this.connect(url, options);
    }

    get_client(): WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient | null {
        return this.state.client;
    }

    is_connected(): boolean {
        return this.state.is_connected;
    }

    is_connecting(): boolean {
        return this.state.is_connecting;
    }

    get_connection_error(): string | null {
        return this.state.connection_error;
    }

    get_state(): Omit<ConnectionState, 'listeners'> {
        const {listeners, ...state} = this.state;
        return state;
    }

    // Subscribe to state changes
    subscribe(listener: (state: ConnectionState) => void): () => void {
        this.state.listeners.add(listener);
        // Return unsubscribe function
        return () => {
            this.state.listeners.delete(listener);
        };
    }

    private update_state(updates: Partial<ConnectionState>): void {
        this.state = {
            ...this.state,
            ...updates,
        };

        // Notify all listeners
        this.state.listeners.forEach(listener => {
            listener(this.state);
        });
    }
}

