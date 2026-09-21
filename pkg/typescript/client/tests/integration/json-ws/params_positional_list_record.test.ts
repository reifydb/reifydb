// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, it} from "vitest";
import {waitForDatabase} from "../setup";
import {Client, JsonWsClient} from "../../../src";
import {expectSingleValueResult} from "./test-helper";
import {ListValue, RecordValue, Utf8Value} from "@reifydb/core";

function regions(): ListValue {
    return new ListValue([
        new RecordValue({id: new Utf8Value('us'), label: new Utf8Value('US')}),
        new RecordValue({id: new Utf8Value('eu'), label: new Utf8Value('EU')}),
    ]);
}

describe('Positional Parameters (List of Records)', () => {
    let wsClient: JsonWsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        wsClient = await Client.connectJsonWs(process.env.REIFYDB_WS_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN,
        });
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            try { wsClient.disconnect(); } catch (error) { console.error('Error during disconnect:', error); }
            wsClient = null;
        }
    });

    it('admin round-trips a List(Record) param', async () => {
        const frames = await wsClient.admin('MAP {result: $1}', [regions()]);
        expectSingleValueResult(frames, regions());
    }, 1000);

    it('command round-trips a List(Record) param', async () => {
        const frames = await wsClient.command('MAP {result: $1}', [regions()]);
        expectSingleValueResult(frames, regions());
    }, 1000);

    it('query round-trips a List(Record) param', async () => {
        const frames = await wsClient.query('MAP {result: $1}', [regions()]);
        expectSingleValueResult(frames, regions());
    }, 1000);
});
