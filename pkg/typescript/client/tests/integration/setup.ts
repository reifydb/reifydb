// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {Client} from "../../src";


export async function wait_for_database(max_retries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < max_retries; i++) {
        let url = process.env.REIFYDB_WS_URL;
        let client = null;
        try {
            client = await Client.connect_ws(url, {timeout_ms: 5000});
            // await client.query('MAP 1;');
            return;
        } catch (error) {
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