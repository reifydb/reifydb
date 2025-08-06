/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Client} from "../../src";


export async function waitForDatabase(maxRetries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
        let url = process.env.REIFYDB_WS_URL;
        let client = null;
        try {
            client = await Client.connect_ws(url, {timeoutMs: 5000});
            // await client.query('MAP 1;');
            return;
        } catch (error) {
            console.log(`❌ Database connection failed on attempt ${i + 1}: ${error.message}`);
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