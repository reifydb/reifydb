// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, JsonWsClient} from "../../../src";

describe('JsonWs call RPC', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `call_jsonws_${suffix}`;
    const greet_binding = `greet_${suffix}`;
    const echo_binding = `echo_${suffix}`;

    let client: JsonWsClient;

    beforeAll(async () => {
        await wait_for_database();
        client = await Client.connect_json_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN});

        await client.admin(`CREATE NAMESPACE ${ns}`);
        await client.admin(`CREATE PROCEDURE ${ns}::greet AS { MAP { result: 42 } }`);
        await client.admin(`CREATE PROCEDURE ${ns}::echo { n: int4 } AS { MAP { out: $n } }`);
        // JsonWsClient decodes json only, so the bindings must be json-format.
        await client.admin(`CREATE WS BINDING ${ns}::greet_ws FOR ${ns}::greet WITH { name: "${greet_binding}", format: "json" }`);
        await client.admin(`CREATE WS BINDING ${ns}::echo_ws FOR ${ns}::echo WITH { name: "${echo_binding}", format: "json" }`);
    }, 30000);

    afterAll(async () => {
        if (client) client.disconnect();
    });

    it('invokes a zero-parameter binding and returns the procedure rows', async () => {
        const rows = await client.call(greet_binding, {});
        expect(rows[0][0].result).toBe('42');
    }, 10000);

    it('passes named params through to the procedure body', async () => {
        const rows = await client.call(echo_binding, {n: 7});
        expect(rows[0][0].out).toBe('7');
    }, 10000);

    it('returns server meta alongside data', async () => {
        const {data, meta} = await client.call_with_meta(greet_binding, {});
        expect(data[0][0].result).toBe('42');
        expect(meta?.fingerprint).toEqual(expect.any(String));
        expect(meta?.duration).toEqual(expect.any(String));
    }, 10000);

    it('rejects a missing required param with INVALID_PARAMS', async () => {
        await expect(client.call(echo_binding, {})).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'INVALID_PARAMS',
        });
    }, 10000);

    it('rejects an unknown binding name with NOT_FOUND', async () => {
        await expect(client.call(`missing_${suffix}`, {})).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'NOT_FOUND',
        });
    }, 10000);
});
