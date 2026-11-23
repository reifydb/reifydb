// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect } from 'vitest';
import { createWasmDb } from '../src/index';
import { wasmBytes } from '../src/load';

interface Diagnostic {
  code: string;
  cause?: Diagnostic | null;
}

const MISSING_NAMESPACE = 'from nosuch::table';

function thrown(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  throw new Error('statement did not throw');
}

function diagnosticOf(error: unknown): Diagnostic | undefined {
  return (error as { diagnostic?: Diagnostic }).diagnostic;
}

async function createRawDb() {
  const mod = await import('../wasm/reifydb_webassembly.js');
  await mod.default({ module_or_path: await wasmBytes() });
  return new mod.WasmDB();
}

describe('statement errors', () => {
  it('throws an Error that carries the diagnostic code', async () => {
    // Without the code a caller can only match on message text, which must stay free to change.
    const db = await createWasmDb();
    const error = thrown(() => db.admin(MISSING_NAMESPACE));
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).not.toBe('');
    expect(diagnosticOf(error)?.code).toBe('CA_002');
    db.free();
  });

  it('keeps the cause chain of a wrapped error', async () => {
    // A flattened diagnostic would report CAST_002 alone and never name the NUMBER_001 it wraps.
    const db = await createWasmDb();
    const diagnostic = diagnosticOf(thrown(() => db.admin('map { cast("abc", int4) }')));
    expect(diagnostic?.code).toBe('CAST_002');
    expect(diagnostic?.cause?.code).toBe('NUMBER_001');
    db.free();
  });

  it('carries the diagnostic on every call that runs a statement', async () => {
    // Each call maps its own error, so one left on the old path must fail here by name.
    const db = await createWasmDb();
    const raw = await createRawDb();
    const calls: Record<string, () => unknown> = {
      admin: () => db.admin(MISSING_NAMESPACE),
      adminWithParams: () => db.adminWithParams(MISSING_NAMESPACE, {}),
      command: () => db.command(MISSING_NAMESPACE),
      commandWithParams: () => db.commandWithParams(MISSING_NAMESPACE, {}),
      query: () => db.query(MISSING_NAMESPACE),
      queryWithParams: () => db.queryWithParams(MISSING_NAMESPACE, {}),
      adminText: () => raw.adminText(MISSING_NAMESPACE),
      commandText: () => raw.commandText(MISSING_NAMESPACE),
      queryText: () => raw.queryText(MISSING_NAMESPACE),
    };
    const codes = Object.fromEntries(
      Object.entries(calls).map(([name, run]) => [name, diagnosticOf(thrown(run))?.code]),
    );
    expect(codes).toEqual(Object.fromEntries(Object.keys(calls).map((name) => [name, 'CA_002'])));
    db.free();
    raw.free();
  });

  it('gives a plain-message error no diagnostic', async () => {
    // An invented diagnostic on a non-engine error would let a caller read a code that never existed.
    const db = await createWasmDb();
    expect(diagnosticOf(thrown(() => db.advanceClock(1)))).toBeUndefined();
    db.free();
  });
});
