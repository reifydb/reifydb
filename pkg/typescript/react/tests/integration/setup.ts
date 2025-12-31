// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Client} from "../../src";
import {cleanup} from '@testing-library/react';
import {afterEach} from 'vitest';

export async function waitForDatabase(maxRetries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
        let url = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';
        let client = null;
        try {
            client = await Client.connect_ws(url, {timeoutMs: 5000});
            // Test connection with simple query - query() requires 3 params
            const result = await client.query(`MAP {test: 1}`, null, []);
            if (!result || !Array.isArray(result)) {
                throw new Error('Invalid query result');
            }
            return;
        } catch (error: any) {
            if (i === maxRetries - 1) {
                throw new Error(`${url} not ready after ${maxRetries} attempts`);
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

// Auto cleanup after each test
afterEach(() => {
    cleanup();
});