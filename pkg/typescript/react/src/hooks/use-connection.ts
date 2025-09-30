import {useContext, useEffect, useState} from 'react';
import {ConnectionConfig} from '../connection/connection';
import {getConnection} from '../connection/connection-pool';
import {ConnectionContext} from '../connection/connection-context';
import {WsClient} from '@reifydb/client';

interface ConnectionState {
    client: WsClient | null;
    isConnected: boolean;
    isConnecting: boolean;
    connectionError: string | null;
}

export function useConnection(overrideConfig?: ConnectionConfig) {
    const contextConnection = useContext(ConnectionContext);

    // Use override config if provided, otherwise use context, otherwise get default
    const [connection] = useState(() => {
        if (overrideConfig) {
            return getConnection(overrideConfig);
        }
        return contextConnection || getConnection();
    });

    const [state, setState] = useState<ConnectionState>(() => connection.getState());

    useEffect(() => {
        // Get initial state immediately
        const currentState = connection.getState();
        setState(currentState);
        
        // Subscribe to connection state changes
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

        // Auto-connect if not connected (only for override configs, context handles its own)
        if (overrideConfig && !connection.isConnected() && !connection.isConnecting()) {
            console.log('[useConnection] Initiating auto-connect for override config...');
            connection.connect().catch(err => {
                console.error('[useConnection] Failed to connect:', err);
            });
        } else if (!contextConnection && !overrideConfig && !connection.isConnected() && !connection.isConnecting()) {
            // Auto-connect for default connection when no context provider
            console.log('[useConnection] Initiating auto-connect for default connection...');
            connection.connect().catch(err => {
                console.error('[useConnection] Failed to connect:', err);
            });
        }

        return unsubscribe;
    }, [connection, overrideConfig, contextConnection]);

    return {
        ...state,
        connect: () => connection.connect(),
        disconnect: () => connection.disconnect(),
        reconnect: () => connection.reconnect(),
    };
}