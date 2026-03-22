// SPDX-License-Identifier: Apache-2.0
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
    // Create singleton on first call
    if (!defaultConnection) {
        const mergedConfig = {...DEFAULT_CONFIG, ...config};
        defaultConnection = new Connection(mergedConfig);
        // Start connection immediately - don't wait for React's useEffect
        defaultConnection.connect().catch(err => {
            console.error('[ConnectionPool] Eager connect failed:', err);
        });
    } else if (config) {
        // Only update config when explicitly provided, to avoid
        // stripping fields (e.g. token) that were set on creation.
        defaultConnection.setConfig({...DEFAULT_CONFIG, ...config});
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