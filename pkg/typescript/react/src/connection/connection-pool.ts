// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import { Connection, ConnectionConfig, DEFAULT_CONFIG } from './connection';

/**
 * Singleton default connection instance.
 * This ensures a single global connection that gets reused and updated.
 */
let defaultConnection: Connection | null = null;

/**
 * Get the singleton connection instance.
 * If a config is provided, the connection's config will be updated via setConfig().
 * @param config - Optional connection configuration
 * @returns The singleton Connection instance
 */
export function getConnection(config?: ConnectionConfig): Connection {
    const effectiveConfig = config ? { ...DEFAULT_CONFIG, ...config } : DEFAULT_CONFIG;

    // Create singleton on first call
    if (!defaultConnection) {
        defaultConnection = new Connection(effectiveConfig);
        // Start connection immediately - don't wait for React's useEffect
        defaultConnection.connect().catch(err => {
            console.error('[ConnectionPool] Eager connect failed:', err);
        });
    } else {
        // Update config on existing connection if provided
        defaultConnection.setConfig(effectiveConfig);
    }

    return defaultConnection;
}

/**
 * Clear the singleton connection
 */
export async function clearConnection(): Promise<void> {
    if (defaultConnection) {
        await defaultConnection.disconnect();
        defaultConnection = null;
    }
}