import React, { createContext, useEffect, useRef, ReactNode } from 'react';
import { Connection, ConnectionConfig } from './connection';
import { getConnection } from './connection-pool';

export const ConnectionContext = createContext<Connection | null>(null);

export interface ConnectionProviderProps {
    config?: ConnectionConfig;
    children: ReactNode;
}

export function ConnectionProvider({ config, children }: ConnectionProviderProps) {
    // Get the singleton connection - this always returns the same instance
    // but updates its config via setConfig()
    const connection = getConnection(config);

    // Track previous config to detect changes
    const prevConfigRef = useRef<string | undefined>(undefined);
    const currentConfigStr = JSON.stringify(config);

    useEffect(() => {
        const configChanged = prevConfigRef.current !== undefined &&
                            prevConfigRef.current !== currentConfigStr;

        if (configChanged && connection.isConnected()) {
            // Config changed while connected - reconnect with new config
            connection.reconnect().catch(err => {
                console.error('[ConnectionProvider] Failed to reconnect:', err);
            });
        } else if (!connection.isConnected() && !connection.isConnecting()) {
            // Auto-connect if not connected
            connection.connect().catch(err => {
                console.error('[ConnectionProvider] Failed to connect:', err);
            });
        }

        // Update previous config reference
        prevConfigRef.current = currentConfigStr;
    }, [currentConfigStr, connection]);

    return (
        <ConnectionContext.Provider value={connection}>
            {children}
        </ConnectionContext.Provider>
    );
}