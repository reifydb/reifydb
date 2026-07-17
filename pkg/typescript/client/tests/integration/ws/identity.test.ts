// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('WS caller identity', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `ident_ws_${suffix}`;
    const binding = `whoami_ws_${suffix}`;
    const alice_token = `tok_alice_${suffix}`;
    const bob_token = `tok_bob_${suffix}`;

    let root: WsClient;
    let alice_id: string;
    let bob_id: string;

    beforeAll(async () => {
        await wait_for_database();
        root = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: AUTH_TOKEN});

        await root.admin(`CREATE USER alice_${suffix}`, {}, []);
        await root.admin(`CREATE AUTHENTICATION FOR alice_${suffix} { method: token; token: '${alice_token}' }`, {}, []);
        await root.admin(`CREATE USER bob_${suffix}`, {}, []);
        await root.admin(`CREATE AUTHENTICATION FOR bob_${suffix} { method: token; token: '${bob_token}' }`, {}, []);

        await root.admin(`CREATE NAMESPACE ${ns}`, {}, []);
        await root.admin(`CREATE PROCEDURE ${ns}::whoami AS { MAP { caller: identity::id() } }`, {}, []);
        // Non-privileged callers need a call policy; `filter { true }` admits any authenticated identity.
        await root.admin(`CREATE PROCEDURE POLICY ON ${ns}::whoami { call: { filter { true } } }`, {}, []);
        // Default (frames) binding format matches the WsClient's default wire format.
        await root.admin(`CREATE WS BINDING ${ns}::whoami_ws FOR ${ns}::whoami WITH { name: "${binding}" }`, {}, []);

        const id_shape = [Shape.object({id: Shape.string()})];
        alice_id = (await root.query(`from system::identities filter { name == 'alice_${suffix}' } map { id }`, {}, id_shape))[0][0].id;
        bob_id = (await root.query(`from system::identities filter { name == 'bob_${suffix}' } map { id }`, {}, id_shape))[0][0].id;
    }, 30000);

    afterAll(async () => {
        if (root) root.disconnect();
    });

    it('distinct users resolve to distinct identity ids', () => {
        expect(alice_id).toBeTruthy();
        expect(bob_id).toBeTruthy();
        expect(alice_id).not.toBe(bob_id);
    });

    it('a called procedure observes the authenticated caller (alice)', async () => {
        const alice = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: alice_token});
        try {
            const frames = await alice.call(binding, {}, [Shape.object({caller: Shape.string()})]);
            // The observed caller must be alice specifically, not root/anonymous and not bob.
            expect(frames[0][0].caller).toBe(alice_id);
            expect(frames[0][0].caller).not.toBe(bob_id);
        } finally {
            alice.disconnect();
        }
    }, 10000);

    it('the same binding observes a different caller (bob)', async () => {
        const bob = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: bob_token});
        try {
            const frames = await bob.call(binding, {}, [Shape.object({caller: Shape.string()})]);
            expect(frames[0][0].caller).toBe(bob_id);
        } finally {
            bob.disconnect();
        }
    }, 10000);
});
