// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('JsonWs caller identity', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `ident_jsonws_${suffix}`;
    const binding = `whoami_jsonws_${suffix}`;
    const aliceToken = `tok_alice_${suffix}`;
    const bobToken = `tok_bob_${suffix}`;

    let root: WsClient;
    let aliceId: string;
    let bobId: string;

    beforeAll(async () => {
        await waitForDatabase();
        root = await Client.connectWs(WS_URL, {timeoutMs: 10000, token: AUTH_TOKEN});

        await root.admin(`CREATE USER alice_${suffix}`, {}, []);
        await root.admin(`CREATE AUTHENTICATION FOR alice_${suffix} { method: token; token: '${aliceToken}' }`, {}, []);
        await root.admin(`CREATE USER bob_${suffix}`, {}, []);
        await root.admin(`CREATE AUTHENTICATION FOR bob_${suffix} { method: token; token: '${bobToken}' }`, {}, []);

        await root.admin(`CREATE NAMESPACE ${ns}`, {}, []);
        await root.admin(`CREATE PROCEDURE ${ns}::whoami AS { MAP { caller: identity::id() } }`, {}, []);
        // Non-privileged callers need a call policy; `filter { true }` admits any authenticated identity.
        await root.admin(`CREATE PROCEDURE POLICY ON ${ns}::whoami { call: { filter { true } } }`, {}, []);
        // JsonWsClient is JSON-only and drops binary frames, so the binding must be json-format:
        // the server encodes the call response per the binding's format, not the client's.
        await root.admin(`CREATE WS BINDING ${ns}::whoami_jsonws FOR ${ns}::whoami WITH { name: "${binding}", format: "json" }`, {}, []);

        const idShape = [Shape.object({id: Shape.string()})];
        aliceId = (await root.query(`from system::identities filter { name == 'alice_${suffix}' } map { id }`, {}, idShape))[0][0].id;
        bobId = (await root.query(`from system::identities filter { name == 'bob_${suffix}' } map { id }`, {}, idShape))[0][0].id;
    }, 30000);

    afterAll(async () => {
        if (root) root.disconnect();
    });

    it('distinct users resolve to distinct identity ids', () => {
        expect(aliceId).toBeTruthy();
        expect(bobId).toBeTruthy();
        expect(aliceId).not.toBe(bobId);
    });

    it('a called procedure observes the authenticated caller (alice)', async () => {
        const alice = await Client.connectJsonWs(WS_URL, {timeoutMs: 10000, token: aliceToken});
        try {
            const rows = await alice.call(binding, {});
            // JsonWsClient returns row-shaped data; the observed caller must be alice, not bob.
            expect(rows[0][0].caller).toBe(aliceId);
            expect(rows[0][0].caller).not.toBe(bobId);
        } finally {
            alice.disconnect();
        }
    }, 10000);

    it('the same binding observes a different caller (bob)', async () => {
        const bob = await Client.connectJsonWs(WS_URL, {timeoutMs: 10000, token: bobToken});
        try {
            const rows = await bob.call(binding, {});
            expect(rows[0][0].caller).toBe(bobId);
        } finally {
            bob.disconnect();
        }
    }, 10000);
});
