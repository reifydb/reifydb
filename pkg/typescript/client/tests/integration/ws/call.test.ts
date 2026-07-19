// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('WS call RPC', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    // Unique per-run suffix so repeated runs against a persistent in-memory testcontainer
    // do not collide on the namespace or on the globally-unique binding names.
    const suffix = `${Date.now()}`;
    const ns = `call_ws_${suffix}`;
    const greet_binding = `greet_${suffix}`;
    const greet_rbcf_binding = `greet_rbcf_${suffix}`;
    const echo_binding = `echo_${suffix}`;

    let client: WsClient;

    beforeAll(async () => {
        await wait_for_database();
        client = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN});

        await client.admin(`CREATE NAMESPACE ${ns}`, {}, []);
        await client.admin(`CREATE PROCEDURE ${ns}::greet AS { MAP { result: 42 } }`, {}, []);
        await client.admin(`CREATE PROCEDURE ${ns}::echo { n: int4 } AS { MAP { out: $n } }`, {}, []);
        // Default binding format is frames; the rbcf binding exercises the binary response path.
        await client.admin(`CREATE WS BINDING ${ns}::greet_ws FOR ${ns}::greet WITH { name: "${greet_binding}" }`, {}, []);
        await client.admin(`CREATE WS BINDING ${ns}::greet_rbcf_ws FOR ${ns}::greet WITH { name: "${greet_rbcf_binding}", format: "rbcf" }`, {}, []);
        await client.admin(`CREATE WS BINDING ${ns}::echo_ws FOR ${ns}::echo WITH { name: "${echo_binding}" }`, {}, []);
    }, 30000);

    afterAll(async () => {
        if (client) client.disconnect();
    });

    it('invokes a zero-parameter binding and returns the procedure frame', async () => {
        const frames = await client.call(greet_binding, {}, [
            Shape.object({result: Shape.number()}),
        ]);

        expect(frames).toHaveLength(1);
        expect(frames[0]).toHaveLength(1);
        expect(frames[0][0].result).toBe(42);
    }, 10000);

    it('passes named params through to the procedure body', async () => {
        // A wrong result here means the client dropped/renamed params on the wire,
        // not merely that call returned something.
        const frames = await client.call(echo_binding, {n: 7}, [
            Shape.object({out: Shape.number()}),
        ]);

        expect(frames[0][0].out).toBe(7);
    }, 10000);

    it('decodes an rbcf-format binding response via the binary path', async () => {
        const frames = await client.call(greet_rbcf_binding, {}, [
            Shape.object({result: Shape.number()}),
        ]);

        expect(frames[0][0].result).toBe(42);
    }, 10000);

    it('returns server meta alongside frames', async () => {
        const {frames, meta} = await client.call_with_meta(greet_binding, {}, [
            Shape.object({result: Shape.number()}),
        ]);

        expect(frames[0][0].result).toBe(42);
        expect(meta?.fingerprint).toEqual(expect.any(String));
        expect(meta?.duration).toEqual(expect.any(String));
    }, 10000);

    it('rejects a missing required param with INVALID_PARAMS', async () => {
        await expect(
            client.call(echo_binding, {}, [Shape.object({out: Shape.number()})])
        ).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'INVALID_PARAMS',
        });
    }, 10000);

    it('rejects an unknown binding name with NOT_FOUND', async () => {
        await expect(
            client.call(`missing_${suffix}`, {}, [Shape.object({})])
        ).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'NOT_FOUND',
        });
    }, 10000);
});
