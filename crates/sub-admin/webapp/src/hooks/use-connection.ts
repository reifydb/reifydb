import { useEffect, useState } from 'react';
import { reifyDBConnection } from '../lib/reifydb-connection';
import { WsClient } from '@reifydb/client';

interface ConnectionState {
  client: WsClient | null;
  isConnected: boolean;
  isConnecting: boolean;
  connectionError: string | null;
}

export function useConnection() {
  const [state, setState] = useState<ConnectionState>(() => reifyDBConnection.getState());

  useEffect(() => {
    // Subscribe to connection state changes
    const unsubscribe = reifyDBConnection.subscribe((newState) => {
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
    if (!reifyDBConnection.isConnected() && !reifyDBConnection.isConnecting()) {
      console.log('[useConnection] Initiating auto-connect...');
      reifyDBConnection.connect().catch(err => {
        console.error('[useConnection] Failed to connect:', err);
      });
    } else {
      console.log('[useConnection] Skipping auto-connect, current state:', {
        isConnected: reifyDBConnection.isConnected(),
        isConnecting: reifyDBConnection.isConnecting()
      });
    }

    return unsubscribe;
  }, []);

  return {
    ...state,
    connect: () => reifyDBConnection.connect(),
    disconnect: () => reifyDBConnection.disconnect(),
    reconnect: () => reifyDBConnection.reconnect(),
  };
}