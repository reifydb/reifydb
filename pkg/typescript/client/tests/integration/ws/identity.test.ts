// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('WS caller identity', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    const suffix = `${Date.now()}_${Math.floor(Math.random() * 1e9)}`;
    const ns = `ident_ws_${suffix}`;
    const binding = `whoami_ws_${suffix}`;
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
        // Default (frames) binding format matches the WsClient's default wire format.
        await root.admin(`CREATE WS BINDING ${ns}::whoami_ws FOR ${ns}::whoami WITH { name: "${binding}" }`, {}, []);

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
        const alice = await Client.connectWs(WS_URL, {timeoutMs: 10000, token: aliceToken});
        try {
            const frames = await alice.call(binding, {}, [Shape.object({caller: Shape.string()})]);
            // The observed caller must be alice specifically, not root/anonymous and not bob.
            expect(frames[0][0].caller).toBe(aliceId);
            expect(frames[0][0].caller).not.toBe(bobId);
        } finally {
            alice.disconnect();
        }
    }, 10000);

    it('the same binding observes a different caller (bob)', async () => {
        const bob = await Client.connectWs(WS_URL, {timeoutMs: 10000, token: bobToken});
        try {
            const frames = await bob.call(binding, {}, [Shape.object({caller: Shape.string()})]);
            expect(frames[0][0].caller).toBe(bobId);
        } finally {
            bob.disconnect();
        }
    }, 10000);

    it('a failed authentication must not subscribe as root', async () => {
        // The client sends Auth fire-and-forget, so a bad token leaves the socket open with the server identity cleared to None.
        const denied = `denied_${suffix}`;
        await root.admin(`CREATE TABLE ${ns}::${denied} { id: int4 }`, {}, []);
        // Denies every non-privileged read, so any row reaching the subscriber proves it ran privileged.
        await root.admin(`CREATE TABLE POLICY deny_${suffix} ON ${ns}::${denied} { from: { filter { false } } }`, {}, []);
        await root.command(`INSERT ${ns}::${denied} [{ id: 1 }]`, {}, []);

        const control = await root.query(`from ${ns}::${denied}`, {}, [Shape.object({id: Shape.number()})]);
        expect(control[0]).toHaveLength(1);

        const rogue = await Client.connectWs(WS_URL, {timeoutMs: 10000, token: `not_a_real_token_${suffix}`});
        try {
            const received: any[] = [];
            const sink = (rows: any[]) => received.push(...rows);
            await rogue.subscribe(
                `from ${ns}::${denied}`,
                null,
                Shape.object({id: Shape.number()}),
                {onInsert: sink, onUpdate: sink, onRemove: sink}
            );
            await new Promise(resolve => setTimeout(resolve, 2000));
            expect(received).toHaveLength(0);
        } finally {
            rogue.disconnect();
        }
    }, 20000);
});
