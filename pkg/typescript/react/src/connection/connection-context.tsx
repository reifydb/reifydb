import React, { createContext, useEffect, useMemo, ReactNode } from 'react';
import { Connection, ConnectionConfig } from './connection';
import { getConnection } from './connection-pool';

export const ConnectionContext = createContext<Connection | null>(null);

export interface ConnectionProviderProps {
    config?: ConnectionConfig;
    children: ReactNode;
}

export function ConnectionProvider({ config, children }: ConnectionProviderProps) {
    const connection = useMemo(() => getConnection(config), [JSON.stringify(config)]);
    
    useEffect(() => {
        // Auto-connect if not connected
        if (!connection.isConnected() && !connection.isConnecting()) {
            console.log('[ConnectionProvider] Initiating auto-connect...');
            connection.connect().catch(err => {
                console.error('[ConnectionProvider] Failed to connect:', err);
            });
        } else {
            console.log('[ConnectionProvider] Skipping auto-connect, current state:', {
                isConnected: connection.isConnected(),
                isConnecting: connection.isConnecting()
            });
        }
    }, [connection]);
    
    return (
        <ConnectionContext.Provider value={connection}>
            {children}
        </ConnectionContext.Provider>
    );
}