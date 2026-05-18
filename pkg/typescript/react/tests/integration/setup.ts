// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {Client} from "../../src";
import {cleanup} from '@testing-library/react';
import {afterEach} from 'vitest';

export async function wait_for_database(max_retries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < max_retries; i++) {
        let url = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
        let client = null;
        try {
            client = await Client.connect_ws(url, {timeout_ms: 5000, token: process.env.REIFYDB_TOKEN});
            // Test connection with simple query - query() requires 3 params
            const result = await client.query(`MAP {test: 1}`, null, []);
            if (!result || !Array.isArray(result)) {
                throw new Error('Invalid query result');
            }
            return;
        } catch (error: any) {
            if (i === max_retries - 1) {
                throw new Error(`${url} not ready after ${max_retries} attempts`);
            }
            await new Promise(resolve => setTimeout(resolve, delay));
        } finally {
            if (client) {
                try {
                    client.disconnect();
                } catch (e) {
                    // Ignore disconnect errors
                }
            }
        }
    }
}

export async function wait_for_database_http(max_retries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < max_retries; i++) {
        let url = process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091';
        try {
            const client = Client.connect_http(url, {token: process.env.REIFYDB_TOKEN});
            const result = await client.query(`MAP {test: 1}`, null, []);
            if (!result || !Array.isArray(result)) {
                throw new Error('Invalid query result');
            }
            return;
        } catch (error: any) {
            if (i === max_retries - 1) {
                throw new Error(`${url} not ready after ${max_retries} attempts`);
            }
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

// Auto cleanup after each test
afterEach(() => {
    cleanup();
});