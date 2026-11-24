// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest';
import { WasmExecutor } from '../src/executor/wasm-executor';

function throwing(error: unknown) {
  return new WasmExecutor({ admin: () => { throw error; } });
}

describe('wasm executor diagnostics', () => {
  it('reads the diagnostic the wasm binding nests under the error', async () => {
    // the binding throws an Error whose parts sit under .diagnostic; missing it leaves the card with only flat text
    const error = Object.assign(new Error('Error AST_005\n  Unexpected token'), {
      diagnostic: {
        code: 'AST_005',
        message: 'Unexpected token',
        rql: 'from t filter { a && b }',
        fragment: { text: '&&', line: 1, column: 18 },
        label: 'found `&&`',
        help: 'Use Operator(CloseCurly) instead',
        notes: [],
      },
    });

    const res = await throwing(error).execute('from t filter { a && b }');

    expect(res.success).toBe(false);
    expect(res.error).toBe('Error AST_005\n  Unexpected token');
    expect(res.diagnostic?.code).toBe('AST_005');
    expect(res.diagnostic?.message).toBe('Unexpected token');
    expect(res.diagnostic?.fragment).toEqual({ text: '&&', line: 1, column: 18 });
    expect(res.diagnostic?.label).toBe('found `&&`');
    expect(res.diagnostic?.help).toBe('Use Operator(CloseCurly) instead');
  });

  it('still reads a diagnostic placed directly on the thrown object', async () => {
    // other executors throw the diagnostic itself; the nested lookup must not drop that shape
    const res = await throwing({ code: 'CA_004', message: 'bad', notes: ['n'] }).execute('x');

    expect(res.diagnostic?.code).toBe('CA_004');
    expect(res.diagnostic?.notes).toEqual(['n']);
  });

  it('gives no diagnostic for a plain error', async () => {
    // a made-up diagnostic would make the card print a code the engine never sent
    const res = await throwing(new Error('network down')).execute('x');

    expect(res.error).toBe('network down');
    expect(res.diagnostic).toBeUndefined();
  });
});
