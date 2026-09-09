// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, JsonWsClient} from "../../../src";
import {Int4Value, Shape, ShapeMismatch} from "@reifydb/core";

// A format=json response carries the type of every column beside its rows. Before it did, a shape on this
// transport only renamed keys: an Int4 stayed the string the wire sent, a value shape was never built, an
// option never arrived as one, and a shape that did not describe the data resolved instead of raising. Each
// test below fails again if the response stops carrying its types.

const ns = `wire_types_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

describe('Wire types [json-ws]', () => {
    let client: JsonWsClient;

    beforeAll(async () => {
        client = await Client.connectJsonWs(process.env.REIFYDB_WS_URL, {timeoutMs: 10000, token: process.env.REIFYDB_TOKEN});
        await client.admin(`create namespace ${ns}`, null, []);
    });

    it('decodes an int4 as a number rather than the string the wire carries', async () => {
        const shape = Shape.object({v: Shape.int4()});
        const [rows] = await client.query('MAP {v: cast(1, int4)}', null, [shape]);
        expect(typeof rows[0].v).toBe('number');
        expect(rows[0].v).toBe(1);
    }, 10000);

    it('decodes an int8 as a bigint so a value past 2^53 survives', async () => {
        const shape = Shape.object({v: Shape.int8()});
        const [rows] = await client.query('MAP {v: 9007199254740993}', null, [shape]);
        expect(typeof rows[0].v).toBe('bigint');
        expect(rows[0].v).toBe(BigInt('9007199254740993'));
    }, 10000);

    it('builds the value object a value shape asks for', async () => {
        const shape = Shape.object({v: Shape.int4Value()});
        const [rows] = await client.query('MAP {v: cast(1, int4)}', null, [shape]);
        expect(rows[0].v).toBeInstanceOf(Int4Value);
        expect(rows[0].v.value).toBe(1);
    }, 10000);

    it('reads a present option as Some and an absent one as None', async () => {
        const shape = Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())});
        await client.admin(`create table ${ns}::opt { id: int4, v: Option(int4) }`, null, []);
        await client.command(`insert ${ns}::opt [{ id: 1, v: 7 }, { id: 2, v: none }]`, null, []);

        const [rows] = await client.query(`from ${ns}::opt sort { id: ASC }`, null, [shape]);

        expect(rows[0].v.isSome()).toBe(true);
        expect(rows[0].v.unwrap()).toBe(7);
        expect(rows[1].v.isNone()).toBe(true);
        expect(() => rows[1].v.unwrap()).toThrow();
    }, 10000);

    it('tells an empty string apart from an absent value on an optional utf8', async () => {
        const shape = Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())});
        await client.admin(`create table ${ns}::opt_str { id: int4, v: Option(utf8) }`, null, []);
        await client.command(`insert ${ns}::opt_str [{ id: 1, v: '' }, { id: 2, v: none }]`, null, []);

        const [rows] = await client.query(`from ${ns}::opt_str sort { id: ASC }`, null, [shape]);

        expect(rows[0].v.isSome()).toBe(true);
        expect(rows[0].v.unwrap()).toBe('');
        expect(rows[1].v.isNone()).toBe(true);
    }, 10000);

    it('raises on a shape that does not describe the data instead of resolving silently', async () => {
        const wrong = Shape.object({v: Shape.booleanValue()});
        await expect(client.query('MAP {v: cast(1, int4)}', null, [wrong])).rejects.toBeInstanceOf(ShapeMismatch);
    }, 10000);

    it('decodes into values carrying their own type when no shape is given', async () => {
        // The wire writes every number as text so nothing is rounded to a double or reformatted on the
        // way. The rows alone therefore cannot say what they hold, so the response's types are what make
        // an Int4 an Int4 here rather than the string "1".
        const [rows] = await client.query('MAP {v: cast(1, int4)}');

        expect(rows[0].v).toBeInstanceOf(Int4Value);
        expect(rows[0].v.type).toBe('Int4');
        expect(rows[0].v.value).toBe(1);
    }, 10000);

    it('keeps an int8 past 2^53 exact without a shape, which a JSON number could not', async () => {
        const [rows] = await client.query('MAP {v: 9007199254740993}');

        expect(rows[0].v.value).toBe(BigInt('9007199254740993'));
    }, 10000);

});
