// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Client} from '@reifydb/client';
import type {WsClient} from '@reifydb/client';

export async function connect(): Promise<WsClient> {
    return Client.connectWs(process.env.REIFYDB_WS_URL!, {
        timeoutMs: 10000,
        token: process.env.REIFYDB_TOKEN,
        format: 'rbcf',
    });
}

export function namespace(prefix: string): string {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

export function poll(predicate: () => Promise<boolean>, timeoutMs = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    const attempt = async (): Promise<void> => {
        if (await predicate()) {
            return;
        }
        if (Date.now() > deadline) {
            throw new Error(`condition not met after ${timeoutMs}ms`);
        }
        await new Promise(resolve => setTimeout(resolve, 50));
        return attempt();
    };
    return attempt();
}
