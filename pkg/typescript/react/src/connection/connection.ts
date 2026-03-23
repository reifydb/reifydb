// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {Client, WsClient, HttpClient, JsonHttpClient, JsonWebsocketClient, type WsClientOptions} from '@reifydb/client';

interface ConnectionState {
    client: WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient | null;
    isConnected: boolean;
    isConnecting: boolean;
    connectionError: string | null;
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
    options: {timeoutMs: 30_000},
};

export const DEFAULT_URL = 'ws://127.0.0.1:8090';

export class Connection {
    private state: ConnectionState = {
        client: null,
        isConnected: false,
        isConnecting: false,
        connectionError: null,
        listeners: new Set(),
    };
    private config: ConnectionConfig;

    constructor(config: ConnectionConfig = DEFAULT_CONFIG) {
        this.config = {...DEFAULT_CONFIG, ...config};
    }

    setConfig(config: ConnectionConfig): void {
        this.config = config;
    }

    getConfig(): ConnectionConfig {
        return this.config;
    }

    async connect(url?: string, options?: Omit<WsClientOptions, 'url'>): Promise<void> {
        // Don't connect if already connected or connecting
        if (this.state.isConnected || this.state.isConnecting) {
            return;
        }

        const connectUrl = url || this.config.url || DEFAULT_CONFIG.url!;
        const connectOptions = {token: this.config.token, ...this.config.options, ...options};

        this.updateState({
            isConnecting: true,
            connectionError: null,
        });

        try {
            const isHttp = connectUrl.startsWith('http://') || connectUrl.startsWith('https://');
            const isJson = this.config.format === 'json';
            let client: WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient;
            if (isHttp) {
                client = isJson
                    ? Client.connect_json_http(connectUrl, connectOptions)
                    : Client.connect_http(connectUrl, connectOptions);
            } else {
                client = isJson
                    ? await Client.connect_json_ws(connectUrl, connectOptions)
                    : await Client.connect_ws(connectUrl, connectOptions);
            }
            this.updateState({
                client,
                isConnected: true,
                isConnecting: false,
                connectionError: null,
            });
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Failed to connect to ReifyDB';
            console.error('[Connection] Connection failed:', errorMessage, err);
            this.updateState({
                client: null,
                isConnected: false,
                isConnecting: false,
                connectionError: errorMessage,
            });
            throw err;
        }
    }

    async disconnect(): Promise<void> {
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

        this.updateState({
            client: null,
            isConnected: false,
            isConnecting: false,
            connectionError: null,
        });
    }

    async reconnect(url?: string, options?: Omit<WsClientOptions, 'url'>): Promise<void> {
        await this.disconnect();
        await this.connect(url, options);
    }

    getClient(): WsClient | HttpClient | JsonHttpClient | JsonWebsocketClient | null {
        return this.state.client;
    }

    isConnected(): boolean {
        return this.state.isConnected;
    }

    isConnecting(): boolean {
        return this.state.isConnecting;
    }

    getConnectionError(): string | null {
        return this.state.connectionError;
    }

    getState(): Omit<ConnectionState, 'listeners'> {
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

    private updateState(updates: Partial<ConnectionState>): void {
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

