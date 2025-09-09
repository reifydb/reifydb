import { useEffect, useState } from 'react';
import { wsConnection } from '../lib/connection.ts';
import { WsClient } from '@reifydb/client';

interface ConnectionState {
  client: WsClient | null;
  isConnected: boolean;
  isConnecting: boolean;
  connectionError: string | null;
}

export function useConnection() {
  const [state, setState] = useState<ConnectionState>(() => wsConnection.getState());

  useEffect(() => {
    // Subscribe to wsConnection state changes
    const unsubscribe = wsConnection.subscribe((newState) => {
      console.log('[useConnection] State update:', {
        isConnected: newState.isConnected,
        isConnecting: newState.isConnecting,
        connectionError: newState.connectionError,
      });
      setState({
        client: newState.client,
        isConnected: newState.isConnected,
        isConnecting: newState.isConnecting,
        connectionError: newState.connectionError,
      });
    });

    // Auto-connect if not connected
    if (!wsConnection.isConnected() && !wsConnection.isConnecting()) {
      console.log('[useConnection] Initiating auto-connect...');
      wsConnection.connect().catch(err => {
        console.error('[useConnection] Failed to connect:', err);
      });
    } else {
      console.log('[useConnection] Skipping auto-connect, current state:', {
        isConnected: wsConnection.isConnected(),
        isConnecting: wsConnection.isConnecting()
      });
    }

    return unsubscribe;
  }, []);

  return {
    ...state,
    connect: () => wsConnection.connect(),
    disconnect: () => wsConnection.disconnect(),
    reconnect: () => wsConnection.reconnect(),
  };
}