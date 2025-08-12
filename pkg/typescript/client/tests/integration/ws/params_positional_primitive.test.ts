/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {Client, WsClient} from "../../../src";
import {waitForDatabase} from "../setup";
import {Schema} from "@reifydb/core";
import {expectSingleResult} from "./test-helper";

describe('Positional Parameters', () => {
    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        try {
            wsClient = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            try {
                wsClient.disconnect();
            } catch (error) {
                console.error('⚠️ Error during disconnect:', error);
            }
            wsClient = null;
        }
    });

    describe('command', () => {

        it('Bool', async () => {
            // @ts-ignore
            const frames = await wsClient.command(
                'MAP $1 as result',
                [true],
                [Schema.object({result: Schema.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

    });

});