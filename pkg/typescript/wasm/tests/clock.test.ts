// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect } from 'vitest';
import { createTestingWasmDb, createWasmDb, type TestingOptions, type WasmDB } from '../src/index';

type Row = Record<string, unknown>;

function rows(db: WasmDB, rql: string): Row[] {
  return db.admin(rql) as Row[];
}

function now(db: WasmDB): string {
  return String(rows(db, 'map { t: datetime::now() }')[0].t);
}

function epochMillis(datetime: string): number {
  return Date.parse(datetime.replace(/\.(\d{3})\d*Z$/, '.$1Z'));
}

describe('createWasmDb', () => {
  it('runs on the real clock', async () => {
    // A seeded or mocked production db froze now() at the epoch and broke every time window and queue lease.
    const db = await createWasmDb();
    expect(Math.abs(epochMillis(now(db)) - Date.now())).toBeLessThan(5_000);
    db.free();
  });

  it('refuses to advance the real clock', async () => {
    // A silent no-op here would let a test believe time moved while it did not.
    const db = await createWasmDb();
    expect(() => db.advanceClock(1)).toThrow('advance clock needs a testing database');
    db.free();
  });
});

describe('createTestingWasmDb', () => {
  it('starts at the seed and does not move by itself', async () => {
    // A test clock that drifts with wall time makes every time-based assertion flaky.
    const db = await createTestingWasmDb({ seed: 0 });
    expect(now(db)).toBe('1970-01-01T00:00:00.000000000Z');
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(now(db)).toBe('1970-01-01T00:00:00.000000000Z');
    db.free();
  });

  it('advances the engine clock by exactly the given millis', async () => {
    // If advance misses the clock the engine reads, now() stays put and time-based tests can never pass.
    const db = await createTestingWasmDb({ seed: 0 });
    db.advanceClock(2_000);
    expect(now(db)).toBe('1970-01-01T00:00:02.000000000Z');
    db.free();
  });

  it('moves the clock the queue procedures read', async () => {
    // Queue backoff must end when the test clock passes it, otherwise a failed item never comes back.
    const db = await createTestingWasmDb({ seed: 0 });
    db.admin(`create namespace qc;
create queue qc::jobs { id: int4 } with { fifo: {}, retry: { attempts: 3, backoff: 2s } };
insert qc::jobs [{ id: 1 }]`);
    const claim = 'call queue::claim("w1", "qc::jobs", 1, duration::seconds(30))';
    const first = rows(db, claim);
    expect(String(first[0].attempt)).toBe('1');
    db.admin(`call queue::fail("${String(first[0].token)}", "503")`);
    expect(rows(db, claim)).toHaveLength(0);
    db.advanceClock(1_999);
    expect(rows(db, claim)).toHaveLength(0);
    db.advanceClock(1);
    const retry = rows(db, claim);
    expect(retry).toHaveLength(1);
    expect(String(retry[0].attempt)).toBe('2');
    db.free();
  });

  it('gives the same random values for the same seed', async () => {
    // An unseeded rng in the testing db puts a different uuid in every run's expected output.
    const one = await createTestingWasmDb({ seed: 7 });
    const two = await createTestingWasmDb({ seed: 7 });
    const uuid = (db: WasmDB) => String(rows(db, 'map { u: uuid::v7() }')[0].u);
    expect(uuid(one)).toBe(uuid(two));
    one.free();
    two.free();
  });

  it('accepts a bigint seed up to the full u64 range', async () => {
    // Bigint seeds past 2^53 are the only way to reach the top of the u64 range.
    const db = await createTestingWasmDb({ seed: 2n ** 64n - 1n });
    db.free();
  });

  it.each([
    ['negative', { seed: -1 }],
    ['fractional', { seed: 1.5 }],
    ['not a number', { seed: Number.NaN }],
    ['past 2^53-1', { seed: 2 ** 53 }],
    ['a string', { seed: '0' }],
    ['missing', {}],
    ['a negative bigint', { seed: -1n }],
    ['a bigint past u64', { seed: 2n ** 64n }],
  ])('rejects a seed that is %s', async (_, options) => {
    // A loose cast would map two different seeds onto the same run without saying so.
    await expect(createTestingWasmDb(options as unknown as TestingOptions)).rejects.toThrow('testing options need seed');
  });

  it.each([
    ['negative', -1],
    ['fractional', 1.5],
    ['not a number', Number.NaN],
    ['past 2^53-1', 2 ** 53],
  ])('rejects advancing by millis that are %s', async (_, millis) => {
    // A wrapped or truncated advance would move time by a different amount than the test asked for.
    const db = await createTestingWasmDb({ seed: 0 });
    expect(() => db.advanceClock(millis)).toThrow('advance clock needs millis');
    expect(now(db)).toBe('1970-01-01T00:00:00.000000000Z');
    db.free();
  });
});
