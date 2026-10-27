// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect } from 'vitest';
import { createWasmDb, type WasmDB } from '../src/index';

async function createChain(): Promise<WasmDB> {
  const db = await createWasmDb();
  db.admin(`create namespace fl;
create table fl::orders { id: int4, customer: utf8, total: int8 };
create deferred view fl::spend { customer: utf8, total: int8 } as {
  from fl::orders
  aggregate { total: math::sum(total) } by { customer }
};
create deferred view fl::big_spenders { customer: utf8 } as {
  from fl::spend
  filter { total > 70 }
  map { customer }
}`);
  return db;
}

describe('awaitAllFlows', () => {
  it('rejects when flows are not caught up within the timeout', async () => {
    // No event loop turn passes before the first step, so the flow cannot have run; a step that never reports false resolves here.
    const db = await createChain();
    db.command(`insert fl::orders [{ id: 1, customer: "grace", total: 75 }]`);

    await expect(db.awaitAllFlows({ timeoutMs: 0 })).rejects.toThrow('deferred flows not caught up');
    db.free();
  });

  it('resolves at once when there are no flows', async () => {
    // A db with no views must never wait for a flow that does not exist.
    const db = await createWasmDb();
    db.admin(`create namespace fl;
create table fl::orders { id: int4, customer: utf8, total: int8 };
insert fl::orders [{ id: 1, customer: "grace", total: 75 }]`);

    await expect(db.awaitAllFlows({ timeoutMs: 0 })).resolves.toBeUndefined();
    db.free();
  });
});
