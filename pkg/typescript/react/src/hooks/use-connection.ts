// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useContext, useEffect, useState} from 'react';
import {ConnectionConfig} from '../connection/connection';
import {get_connection} from '../connection/connection-pool';
import {ConnectionContext} from '../connection/connection-context';
import {WsClient, HttpClient, JsonHttpClient, JsonWsClient} from '@reifydb/client';

interface ConnectionState {
    client: WsClient | HttpClient | JsonHttpClient | JsonWsClient | null;
    is_connected: boolean;
    is_connecting: boolean;
    connection_error: string | null;
}

export function useConnection(override_config?: ConnectionConfig) {
    const context_connection = useContext(ConnectionContext);

    // Use override config if provided, otherwise use context, otherwise get default
    const [connection] = useState(() => {
        if (override_config) {
            return get_connection(override_config);
        }
        if (context_connection) {
            return context_connection;
        }
        return get_connection();
    });

    const [state, setState] = useState<ConnectionState>(() => connection.get_state());

    useEffect(() => {
        // Get initial state immediately
        const current_state = connection.get_state();
        setState(current_state);

        // Subscribe to connection state changes
        const unsubscribe = connection.subscribe((new_state) => {
            setState({
                client: new_state.client,
                is_connected: new_state.is_connected,
                is_connecting: new_state.is_connecting,
                connection_error: new_state.connection_error,
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