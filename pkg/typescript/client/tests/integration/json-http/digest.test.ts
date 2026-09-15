// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, HttpClient, JsonHttpClient} from "../../../src";
import {DigestValue, DurationValue, Float8Value, NoneValue, Option, digestType} from "@reifydb/core";

const ns = `digest_json_http_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

const DIGESTS = `from ${ns}::samples aggregate { amounts: stats::digest(amount, 0.01), hits: stats::digest(hit, 0.05), latencies: stats::digest(latency, 0.001), maybes: stats::digest(maybe, 0.01) } by { grp } sort { grp: asc }`;

const PERCENTILES = `from ${ns}::samples aggregate { amounts: stats::digest(amount, 0.01), coarse: stats::digest(amount, 0.05), hits: stats::digest(hit, 0.05), latencies: stats::digest(latency, 0.001) } by { grp } map { grp, p99: stats::approx_percentile(amounts, 0.99), p50: stats::approx_percentile(amounts, 0.5), coarse_p99: stats::approx_percentile(coarse, 0.99), hits_p99: stats::approx_percentile(hits, 0.99), latency_p99: stats::approx_percentile(latencies, 0.99) } sort { grp: asc }`;

const FLOAT8_DIGEST = digestType("Float8", 10_000);

function connect(): JsonHttpClient {
    return Client.connectJsonHttp(process.env.REIFYDB_HTTP_URL, {
        timeoutMs: 10000,
        token: process.env.REIFYDB_TOKEN,
    });
}

beforeAll(async () => {
    const client = connect();
    await client.admin(`create namespace ${ns}`, null, []);
    await client.admin(`create table ${ns}::samples { grp: utf8, amount: float8, hit: int4, latency: duration, maybe: Option(float8) }`, null, []);
    await client.command(`insert ${ns}::samples [
        { grp: 'a', amount: 1.0, hit: 10, latency: 5ms, maybe: none },
        { grp: 'a', amount: 2.0, hit: 20, latency: 50ms, maybe: none },
        { grp: 'a', amount: 3.0, hit: 30, latency: 500ms, maybe: none },
        { grp: 'a', amount: 4.0, hit: 40, latency: 5s, maybe: none },
        { grp: 'a', amount: 100.0, hit: 1000, latency: 50s, maybe: none },
        { grp: 'b', amount: 7.0, hit: 70, latency: 7ms, maybe: 7.0 },
        { grp: 'b', amount: 70.0, hit: 700, latency: 70ms, maybe: 70.0 }
    ]`, null, []);
}, 30000);

describe('Digest [json-http]', () => {
    const client = connect();

    it('decodes a digest per group with its inner type, accuracy and count', async () => {
        // A wrong type tag, accuracy field or bucket walk in the decoder shows up as a wrong inner, accuracy or count.
        const [rows] = await client.query(DIGESTS, null, []);
        expect(rows.map((row: any) => row.grp.value)).toEqual(["a", "b"]);
        const [a, b] = rows as any[];

        expect(a.amounts).toBeInstanceOf(DigestValue);
        expect(a.amounts.type).toEqual(FLOAT8_DIGEST);
        expect(a.amounts.inner).toBe("Float8");
        expect(a.amounts.accuracy).toBe(10_000);
        expect(a.amounts.count).toBe(5n);
        expect(a.amounts.toString()).toBe("digest(n: 5)");

        expect(a.hits).toBeInstanceOf(DigestValue);
        expect(a.hits.type).toEqual(digestType("Int4", 50_000));
        expect(a.hits.count).toBe(5n);

        expect(a.latencies).toBeInstanceOf(DigestValue);
        expect(a.latencies.type).toEqual(digestType("Duration", 1_000));
        expect(a.latencies.count).toBe(5n);

        expect(b.amounts.count).toBe(2n);
        expect(b.amounts.toString()).toBe("digest(n: 2)");
        expect(b.maybes).toBeInstanceOf(DigestValue);
        expect(b.maybes.type).toEqual(FLOAT8_DIGEST);
        expect(b.maybes.count).toBe(2n);
    }, 10000);

    it('matches the bytes the server builds again from reordered rows, a merge and an echoed parameter', async () => {
        // Canonical bytes must not depend on add order, a merge pass or a trip through the parameter decoder.
        const [rows] = await client.query(DIGESTS, null, []);
        const [reordered] = await client.query(
            `from ${ns}::samples sort { amount: desc } aggregate { amounts: stats::digest(amount, 0.01) } by { grp } sort { grp: asc }`,
            null, []
        );
        const [merged] = await client.query(
            `from ${ns}::samples aggregate { amounts: stats::digest(amount, 0.01) } by { grp } aggregate { amounts: stats::digest(amounts) } by { grp } sort { grp: asc }`,
            null, []
        );

        expect(reordered).toHaveLength(2);
        expect(merged).toHaveLength(2);
        for (const [i, row] of (rows as any[]).entries()) {
            const bytes = row.amounts.asBytes();
            expect(reordered[i].amounts.asBytes()).toEqual(bytes);
            expect(merged[i].amounts.asBytes()).toEqual(bytes);

            const [[echoed]] = await client.query('map { echoed: $1 }', [row.amounts], []);
            expect(echoed.echoed).toBeInstanceOf(DigestValue);
            expect(echoed.echoed.asBytes()).toEqual(bytes);
        }
    }, 10000);

    it('reads the same percentile from a positional digest parameter as from one query', async () => {
        // A digest that loses a bucket or its accuracy on the way out reads a different p than the server alone.
        const [rows] = await client.query(DIGESTS, null, []);
        const [truth] = await client.query(PERCENTILES, null, []);

        expect(truth[0].p99.value).not.toBe(truth[0].p50.value);
        expect(truth[0].p99.value).not.toBe(truth[0].coarse_p99.value);
        expect(Math.abs(truth[0].p99.value - 100)).toBeLessThanOrEqual(1);
        expect(Math.abs(truth[0].p50.value - 3)).toBeLessThanOrEqual(0.03);
        expect(Math.abs(truth[1].p99.value - 70)).toBeLessThanOrEqual(0.7);

        for (const [i, row] of (rows as any[]).entries()) {
            for (const param of [row.amounts, Option.some(row.amounts)]) {
                const [[read]] = await client.query(
                    'map { p99: stats::approx_percentile($1, 0.99), p50: stats::approx_percentile($1, 0.5) }',
                    [param], []
                );
                expect(read.p99).toBeInstanceOf(Float8Value);
                expect(read.p99.value).toBe(truth[i].p99.value);
                expect(read.p50.value).toBe(truth[i].p50.value);
            }
        }
    }, 10000);

    it('reads the same percentile from a named digest parameter as from one query', async () => {
        // A named parameter goes through its own encode path, so a digest must reach the server intact there too.
        const [rows] = await client.query(DIGESTS, null, []);
        const [truth] = await client.query(PERCENTILES, null, []);

        expect(Math.abs(truth[0].hits_p99.value - 1000)).toBeLessThanOrEqual(50);

        for (const [i, row] of (rows as any[]).entries()) {
            const [[read]] = await client.query(
                'map { p99: stats::approx_percentile($amounts, 0.99), p50: stats::approx_percentile($amounts, 0.5), hits_p99: stats::approx_percentile($hits, 0.99) }',
                {amounts: row.amounts, hits: row.hits}, []
            );
            expect(read.p99.value).toBe(truth[i].p99.value);
            expect(read.p50.value).toBe(truth[i].p50.value);
            expect(read.hits_p99).toBeInstanceOf(Float8Value);
            expect(read.hits_p99.value).toBe(truth[i].hits_p99.value);
        }
    }, 10000);

    it('reads a percentile of a duration digest parameter back as a duration', async () => {
        // A duration digest must answer in Duration, never in the Float8 nanoseconds its buckets hold.
        const [rows] = await client.query(DIGESTS, null, []);
        const [truth] = await client.query(PERCENTILES, null, []);

        expect(truth[0].latency_p99).toBeInstanceOf(DurationValue);
        expect(truth[0].latency_p99.getMonths()).toBe(0);
        expect(truth[0].latency_p99.getDays()).toBe(0);
        const nanos = truth[0].latency_p99.getNanos();
        const off = nanos > 50_000_000_000n ? nanos - 50_000_000_000n : 50_000_000_000n - nanos;
        expect(off <= 50_000_000n).toBe(true);

        for (const [i, row] of (rows as any[]).entries()) {
            const [[positional]] = await client.query('map { p99: stats::approx_percentile($1, 0.99) }', [row.latencies], []);
            const [[named]] = await client.query('map { p99: stats::approx_percentile($d, 0.99) }', {d: row.latencies}, []);
            for (const read of [positional, named]) {
                expect(read.p99).toBeInstanceOf(DurationValue);
                expect(read.p99.equals(truth[i].latency_p99)).toBe(true);
            }
        }
    }, 10000);

    it('decodes an all-none group as none and sends it back as a none parameter', async () => {
        // An all-none group must stay a typed none both ways, never an empty digest or an untyped none.
        const [rows] = await client.query(DIGESTS, null, []);
        const none = rows[0].maybes;
        expect(none).toBeInstanceOf(NoneValue);
        expect(none.innerType).toEqual(FLOAT8_DIGEST);

        const sent = [
            {rql: 'map { p99: stats::approx_percentile($1, 0.99), echoed: $1 }', params: [none]},
            {rql: 'map { p99: stats::approx_percentile($1, 0.99), echoed: $1 }', params: [Option.none(FLOAT8_DIGEST)]},
            {rql: 'map { p99: stats::approx_percentile($d, 0.99), echoed: $d }', params: {d: none}},
        ];
        for (const {rql, params} of sent) {
            const [[read]] = await client.query(rql, params, []);
            expect(read.p99).toBeInstanceOf(NoneValue);
            expect(read.p99.innerType).toBe("Float8");
            expect(read.echoed).toBeInstanceOf(NoneValue);
            expect(read.echoed.innerType).toEqual(FLOAT8_DIGEST);
        }
    }, 10000);

    it('rejects a malformed digest parameter instead of reading it as none', async () => {
        // Truncated bytes, or a type accuracy that disagrees with the bytes, must fail on the server, not read as none.
        const [rows] = await client.query(DIGESTS, null, []);
        const good = rows[0].amounts.encode();
        const rql = 'map { p99: stats::approx_percentile($1, 0.99) }';

        const [[control]] = await client.query(rql, [rows[0].amounts], []);
        expect(control.p99).toBeInstanceOf(Float8Value);

        const truncatedHex = good.value.slice(0, -2);
        const cases = [
            {param: {encode: () => ({type: good.type, value: truncatedHex})}, text: `cannot parse '${truncatedHex}' as Digest(Float8, 0.01)`},
            {param: {encode: () => ({type: digestType("Float8", 50_000), value: good.value})}, text: `cannot parse '${good.value}' as Digest(Float8, 0.05)`},
        ];
        for (const {param, text} of cases) {
            await expect(client.query(rql, [param], [])).rejects.toMatchObject({
                name: "ReifyError",
                code: "INVALID_PARAMS",
                message: expect.stringContaining(text),
            });
        }
    }, 10000);
});

describe('Digest across transports [json-http]', () => {
    it('gives byte-equal digests over json and rbcf', async () => {
        // The json row cell and the rbcf binary column must carry the same canonical bytes for every digest.
        const json = connect();
        const rbcf: HttpClient = Client.connectHttp(process.env.REIFYDB_HTTP_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN,
            format: "rbcf",
        });
        const [jsonRows] = await json.query(DIGESTS, null, []);
        const [binaryRows] = await rbcf.query(DIGESTS, null, []);
        expect(binaryRows).toHaveLength(jsonRows.length);
        for (const [i, row] of (jsonRows as any[]).entries()) {
            for (const column of ["amounts", "hits", "latencies"]) {
                expect(binaryRows[i][column]).toBeInstanceOf(DigestValue);
                expect(binaryRows[i][column].asBytes()).toEqual(row[column].asBytes());
            }
        }
        expect(binaryRows[0].maybes).toBeInstanceOf(NoneValue);
        expect(jsonRows[0].maybes).toBeInstanceOf(NoneValue);
        expect(binaryRows[1].maybes.asBytes()).toEqual(jsonRows[1].maybes.asBytes());
    }, 10000);
});
