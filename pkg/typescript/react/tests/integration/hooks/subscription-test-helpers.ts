// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type {ChangeEvent} from "../../../src/hooks/use-subscription-executor";

// Re-export utilities from client test helpers
export {
    create_test_table_name, create_test_table, wait_for_callback, create_callback_tracker
} from '../../../../client/tests/integration/ws/subscription-helpers';

/**
 * Wait for a specific number of change events
 */
export async function wait_for_change_count(
    get_changes: () => ChangeEvent<any>[],
    expected_count: number,
    timeout_ms: number = 5000
): Promise<void> {
    const start_time = Date.now();
    while (Date.now() - start_time < timeout_ms) {
        if (get_changes().length >= expected_count) {
            return;
        }
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error(`Timeout waiting for ${expected_count} changes. Got ${get_changes().length}`);
}


/**
 * Create a test table specifically for hook testing
 * Uses a consistent pattern for test tables in hook tests
 * Gets the client from the connection pool automatically
 */
export async function create_test_table_for_hook(
    prefix: string,
    columns: string[]
): Promise<string> {
    const {get_connection} = await import('../../../src');
    const {
        create_test_table_name,
        create_test_table
    } = await import('../../../../client/tests/integration/ws/subscription-helpers');

    const conn = get_connection({url: process.env.REIFYDB_WS_URL, token: process.env.REIFYDB_TOKEN});

    // Wait for connection to be fully established
    // Handle race conditions where connection might be in progress
    const max_wait = 5000;
    const start_time = Date.now();
    while (Date.now() - start_time < max_wait) {
        if (conn.is_connected() && conn.get_client()) {
            break;
        }
        if (!conn.is_connecting() && !conn.is_connected()) {
            await conn.connect();
        }
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    const client = conn.get_client();
    if (!client) {
        throw new Error('Client is not connected after 5 seconds');
    }

    const table_name = create_test_table_name(prefix);
    await create_test_table(client, table_name, columns);
    return table_name;
}
