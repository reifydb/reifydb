import { Connection, ConnectionConfig, DEFAULT_CONFIG } from './connection';

/**
 * Connection cache that stores connections by their configuration.
 * This ensures that connections with the same configuration share the same instance.
 */
const connectionCache = new Map<string, Connection>();

/**
 * Get or create a connection for the given configuration.
 * If a connection with the same config already exists, it will be reused.
 * @param config - Optional connection configuration
 * @returns Connection instance for the given configuration
 */
export function getConnection(config?: ConnectionConfig): Connection {
    const effectiveConfig = config ? { ...DEFAULT_CONFIG, ...config } : DEFAULT_CONFIG;
    const key = JSON.stringify(effectiveConfig);
    
    if (!connectionCache.has(key)) {
        connectionCache.set(key, new Connection(effectiveConfig));
    }
    
    return connectionCache.get(key)!;
}

/**
 * Clear a specific connection from the cache
 * @param config - Configuration of the connection to clear
 */
export async function clearConnection(config?: ConnectionConfig): Promise<void> {
    const effectiveConfig = config ? { ...DEFAULT_CONFIG, ...config } : DEFAULT_CONFIG;
    const key = JSON.stringify(effectiveConfig);
    
    const connection = connectionCache.get(key);
    if (connection) {
        await connection.disconnect();
        connectionCache.delete(key);
    }
}

/**
 * Clear all cached connections
 */
export async function clearAllConnections(): Promise<void> {
    const disconnectPromises = [];
    for (const connection of connectionCache.values()) {
        disconnectPromises.push(connection.disconnect());
    }
    await Promise.all(disconnectPromises);
    connectionCache.clear();
}