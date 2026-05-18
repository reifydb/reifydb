// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import React, { createContext, useEffect, useRef, ReactNode } from 'react';
import { Connection, ConnectionConfig } from './connection';
import { get_connection } from './connection-pool';

export const ConnectionContext = createContext<Connection | null>(null);

export interface ConnectionProviderProps {
    config?: ConnectionConfig;
    children: ReactNode;
}

export function ConnectionProvider({ config, children }: ConnectionProviderProps) {
    // Get the singleton connection - this always returns the same instance
    // but updates its config via set_config()
    const connection = get_connection(config);

    // Track previous config to detect changes
    const prev_config_ref = useRef<string | undefined>(undefined);
    const current_config_str = JSON.stringify(config);

    useEffect(() => {
        const config_changed = prev_config_ref.current !== undefined &&
                            prev_config_ref.current !== current_config_str;

        if (config_changed && connection.is_connected()) {
            // Config changed while connected - reconnect with new config
            connection.reconnect().catch(err => {
                console.error('[ConnectionProvider] Failed to reconnect:', err);
            });
        } else if (!connection.is_connected() && !connection.is_connecting()) {
            // Auto-connect if not connected
            connection.connect().catch(err => {
                console.error('[ConnectionProvider] Failed to connect:', err);
            });
        }

        // Update previous config reference
        prev_config_ref.current = current_config_str;
    }, [current_config_str, connection]);

    return (
        <ConnectionContext.Provider value={connection}>
            {children}
        </ConnectionContext.Provider>
    );
}