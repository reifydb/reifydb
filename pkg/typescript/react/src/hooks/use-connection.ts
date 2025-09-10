import { useEffect, useState } from 'react';
import { connection } from '../connection/connection';
import { WsClient } from '@reifydb/client';

interface ConnectionState {
  client: WsClient | null;
  isConnected: boolean;
  isConnecting: boolean;
  connectionError: string | null;
}

export function useConnection() {
  const [state, setState] = useState<ConnectionState>(() => connection.getState());

  useEffect(() => {
    // Subscribe to wsConnection state changes
    const unsubscribe = connection.subscribe((newState) => {
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
    if (!connection.isConnected() && !connection.isConnecting()) {
      console.log('[useConnection] Initiating auto-connect...');
      connection.connect().catch(err => {
        console.error('[useConnection] Failed to connect:', err);
      });
    } else {
      console.log('[useConnection] Skipping auto-connect, current state:', {
        isConnected: connection.isConnected(),
        isConnecting: connection.isConnecting()
      });
    }

    return unsubscribe;
  }, []);

  return {
    ...state,
    connect: () => connection.connect(),
    disconnect: () => connection.disconnect(),
    reconnect: () => connection.reconnect(),
  };
}