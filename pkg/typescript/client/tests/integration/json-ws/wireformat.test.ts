// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWsClient, WsClient} from "../../../src";

// JsonWsClient is json-only: it decodes text json rows and silently drops binary frames. Before the
// server honored the client's requested format, calling a frames-format binding returned the raw
// {frames} envelope (no rows) and calling an rbcf-format binding dropped the binary response
// entirely (the request would hang). Now the client always requests json and the server honors it,
// so a JsonWsClient can invoke a binding of any declared format.
describe('JsonWs wire-format adherence', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `wf_jsonws_${suffix}`;
    const framesBinding = `wf_frames_${suffix}`;
    const rbcfBinding = `wf_rbcf_${suffix}`;

    let root: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
        root = await Client.connectWs(WS_URL, {timeoutMs: 10000, token: AUTH_TOKEN});

        await root.admin(`CREATE NAMESPACE ${ns}`, {}, []);
        await root.admin(`CREATE PROCEDURE ${ns}::greet AS { MAP { result: 42 } }`, {}, []);
        await root.admin(`CREATE WS BINDING ${ns}::greet_frames FOR ${ns}::greet WITH { name: "${framesBinding}", format: "frames" }`, {}, []);
        await root.admin(`CREATE WS BINDING ${ns}::greet_rbcf FOR ${ns}::greet WITH { name: "${rbcfBinding}", format: "rbcf" }`, {}, []);
    }, 30000);

    afterAll(async () => {
        if (root) root.disconnect();
    });

    it('decodes a frames-format binding as json rows', async () => {
        const client = await Client.connectJsonWs(WS_URL, {timeoutMs: 10000, token: AUTH_TOKEN});
        try {
            const rows = await client.call(framesBinding, {});
            expect(rows[0][0].result).toBe('42');
        } finally {
            client.disconnect();
        }
    }, 10000);

    it('decodes an rbcf-format binding as json rows (binary would otherwise be dropped)', async () => {
        const client = await Client.connectJsonWs(WS_URL, {timeoutMs: 10000, token: AUTH_TOKEN});
        try {
            const rows = await client.call(rbcfBinding, {});
            expect(rows[0][0].result).toBe('42');
        } finally {
            client.disconnect();
        }
    }, 10000);
});
