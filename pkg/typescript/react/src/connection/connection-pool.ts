// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { Connection, ConnectionConfig, DEFAULT_CONFIG } from './connection';

/**
 * Singleton default connection instance.
 * This ensures a single global connection that gets reused and updated.
 */
let default_connection: Connection | null = null;

/**
 * Get the singleton connection instance.
 * If a config is provided, the connection's config will be updated via set_config().
 * @param config - Optional connection configuration
 * @returns The singleton Connection instance
 */
export function get_connection(config?: ConnectionConfig): Connection {
    // Create singleton on first call
    if (!default_connection) {
        const merged_config = {...DEFAULT_CONFIG, ...config};
        default_connection = new Connection(merged_config);
        // Start connection immediately - don't wait for React's useEffect
        default_connection.connect().catch(err => {
            console.error('[ConnectionPool] Eager connect failed:', err);
        });
    } else if (config) {
        // Only update config when explicitly provided, to avoid
        // stripping fields (e.g. token) that were set on creation.
        default_connection.set_config({...DEFAULT_CONFIG, ...config});
    }

    return default_connection;
}

/**
 * Clear the singleton connection
 */
export async function clear_connection(): Promise<void> {
    if (default_connection) {
        await default_connection.disconnect();
        default_connection = null;
    }
}