import { Client, WsClient, type WsClientOptions } from '@reifydb/client';
import { REIFYDB_CONFIG } from '../config';

interface ConnectionState {
  client: WsClient | null;
  isConnected: boolean;
  isConnecting: boolean;
  connectionError: string | null;
  listeners: Set<(state: ConnectionState) => void>;
}

class WsConnection {
  private static instance: WsConnection;
  private state: ConnectionState = {
    client: null,
    isConnected: false,
    isConnecting: false,
    connectionError: null,
    listeners: new Set(),
  };

  private constructor() {}

  static getInstance(): WsConnection {
    if (!WsConnection.instance) {
      WsConnection.instance = new WsConnection();
    }
    return WsConnection.instance;
  }

  async connect(url: string = REIFYDB_CONFIG.getWebSocketUrl(), options?: Omit<WsClientOptions, 'url'>): Promise<void> {
    // Don't connect if already connected or connecting
    if (this.state.isConnected || this.state.isConnecting) {
      console.log('[WsConnection] Already connected or connecting, skipping connection attempt');
      return;
    }

    console.log('[WsConnection] Attempting to connect to:', url);
    this.updateState({
      isConnecting: true,
      connectionError: null,
    });

    try {
      const client = await Client.connect_ws(url, {
        timeoutMs: 1000,
        ...options
      });
      
      console.log('[WsConnection] Successfully connected to WebSocket');
      this.updateState({
        client,
        isConnected: true,
        isConnecting: false,
        connectionError: null,
      });
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to connect to ReifyDB';
      console.error('[WsConnection] Connection failed:', errorMessage, err);
      this.updateState({
        client: null,
        isConnected: false,
        isConnecting: false,
        connectionError: errorMessage,
      });
      throw err;
    }
  }

  disconnect(): void {
    if (this.state.client) {
      try {
        this.state.client.disconnect();
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
    this.disconnect();
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
    const { listeners, ...state } = this.state;
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

// Export singleton instance
export const reifyDBConnection = WsConnection.getInstance();