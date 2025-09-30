import {Client, WsClient, type WsClientOptions} from '@reifydb/client';

interface ConnectionState {
    client: WsClient | null;
    isConnected: boolean;
    isConnecting: boolean;
    connectionError: string | null;
    listeners: Set<(state: ConnectionState) => void>;
}

export interface ConnectionConfig {
    url?: string;
    options?: Omit<WsClientOptions, 'url'>;
}

export const DEFAULT_CONFIG: ConnectionConfig = {
    url: 'ws://127.0.0.1:8090',
    options: {
        timeoutMs: 1000,
    }
};

export class Connection {
    private state: ConnectionState = {
        client: null,
        isConnected: false,
        isConnecting: false,
        connectionError: null,
        listeners: new Set(),
    };
    private config: ConnectionConfig;

    constructor(config?: ConnectionConfig) {
        this.config = { ...DEFAULT_CONFIG, ...config };
    }

    setConfig(config: ConnectionConfig): void {
        this.config = {...DEFAULT_CONFIG, ...config};
    }

    getConfig(): ConnectionConfig {
        return this.config;
    }

    async connect(url?: string, options?: Omit<WsClientOptions, 'url'>): Promise<void> {
        // Don't connect if already connected or connecting
        if (this.state.isConnected || this.state.isConnecting) {
            console.debug('[Connection] Already connected or connecting, skipping wsConnection attempt');
            return;
        }

        const connectUrl = url || this.config.url || DEFAULT_CONFIG.url!;
        const connectOptions = {...this.config.options, ...options};

        console.debug('[Connection] Attempting to connect to:', connectUrl);
        this.updateState({
            isConnecting: true,
            connectionError: null,
        });

        try {
            const client = await Client.connect_ws(connectUrl, connectOptions);

            console.debug('[Connection] Successfully connected to WebSocket');
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
                this.state.client.disconnect();
                // Small delay to ensure WebSocket closes cleanly
                await new Promise(resolve => setTimeout(resolve, 10));
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

    getClient(): WsClient | null {
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

