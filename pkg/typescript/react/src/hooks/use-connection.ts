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
            setState({
                client: newState.client,
                isConnected: newState.isConnected,
                isConnecting: newState.isConnecting,
                connectionError: newState.connectionError,
            });
        });

        // No auto-connect - ConnectionProvider handles all auto-connection
        // Users must either wrap with ConnectionProvider or manually call connect()

        return unsubscribe;
    }, [connection]);

    return {
        ...state,
        connect: () => connection.connect(),
        disconnect: () => connection.disconnect(),
        reconnect: () => connection.reconnect(),
    };
}