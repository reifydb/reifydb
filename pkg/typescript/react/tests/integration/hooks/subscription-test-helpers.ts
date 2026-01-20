// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import type {ChangeEvent} from "../../../src/hooks/use-subscription-executor";

// Re-export utilities from client test helpers
export {
    createTestTableName, createTestTable, waitForCallback, createCallbackTracker
} from '../../../../client/tests/integration/ws/subscription-helpers';

/**
 * Wait for a specific number of change events
 */
export async function waitForChangeCount(
    getChanges: () => ChangeEvent<any>[],
    expectedCount: number,
    timeoutMs: number = 5000
): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
        if (getChanges().length >= expectedCount) {
            return;
        }
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error(`Timeout waiting for ${expectedCount} changes. Got ${getChanges().length}`);
}


/**
 * Create a test table specifically for hook testing
 * Uses a consistent pattern for test tables in hook tests
 * Gets the client from the connection pool automatically
 */
export async function createTestTableForHook(
    prefix: string,
    columns: string[]
): Promise<string> {
    const {getConnection} = await import('../../../src');
    const {
        createTestTableName,
        createTestTable
    } = await import('../../../../client/tests/integration/ws/subscription-helpers');

    const conn = getConnection();

    // Wait for connection to be fully established
    // Handle race conditions where connection might be in progress
    const maxWait = 5000;
    const startTime = Date.now();
    while (Date.now() - startTime < maxWait) {
        if (conn.isConnected() && conn.getClient()) {
            break;
        }
        if (!conn.isConnecting() && !conn.isConnected()) {
            await conn.connect();
        }
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    const client = conn.getClient();
    if (!client) {
        throw new Error('Client is not connected after 5 seconds');
    }

    const tableName = createTestTableName(prefix);
    await createTestTable(client, tableName, columns);
    return tableName;
}
