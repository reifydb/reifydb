// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

// The server encodes a call response in the CLIENT's requested wire format, falling back to the
// binding's declared format only when the client asks for none. These tests prove that by pointing
// a client at a binding whose declared format differs from the client's decoder: before the server
// honored the client format, WsClient (which decodes frames/rbcf, not json rows) returned empty or
// wrong data against a json binding, and a json-configured WsClient returned the raw envelope
// against a frames binding. Each case below would fail without the server-side format honoring.
describe('WS wire-format adherence', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `wf_ws_${suffix}`;
    const json_binding = `wf_json_${suffix}`;
    const frames_binding = `wf_frames_${suffix}`;

    let root: WsClient;

    beforeAll(async () => {
        await wait_for_database();
        root = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN});

        await root.admin(`CREATE NAMESPACE ${ns}`, {}, []);
        await root.admin(`CREATE PROCEDURE ${ns}::greet AS { MAP { result: 42 } }`, {}, []);
        await root.admin(`CREATE WS BINDING ${ns}::greet_json FOR ${ns}::greet WITH { name: "${json_binding}", format: "json" }`, {}, []);
        await root.admin(`CREATE WS BINDING ${ns}::greet_frames FOR ${ns}::greet WITH { name: "${frames_binding}", format: "frames" }`, {}, []);
    }, 30000);

    afterAll(async () => {
        if (root) root.disconnect();
    });

    it('a frames client decodes a JSON-format binding', async () => {
        const client = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN, format: 'frames'});
        try {
            const frames = await client.call(json_binding, {}, [Shape.object({result: Shape.number()})]);
            expect(frames[0][0].result).toBe(42);
        } finally {
            client.disconnect();
        }
    }, 10000);

    it('a json client decodes a frames-format binding', async () => {
        const client = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN, format: 'json'});
        try {
            // The json wire format returns row values as json scalars (strings), unlike the
            // shape-coerced frames/rbcf paths; the point here is that the correct value arrives.
            const frames = await client.call(frames_binding, {}, [Shape.object({result: Shape.number()})]);
            expect(frames[0][0].result).toBe('42');
        } finally {
            client.disconnect();
        }
    }, 10000);

    it('an rbcf client decodes a JSON-format binding via the binary path', async () => {
        const client = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN, format: 'rbcf'});
        try {
            const frames = await client.call(json_binding, {}, [Shape.object({result: Shape.number()})]);
            expect(frames[0][0].result).toBe(42);
        } finally {
            client.disconnect();
        }
    }, 10000);
});
