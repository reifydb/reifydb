// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect } from 'vitest';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';
import { isGzip, wasmBytes } from '../src/load';

const WASM_HEADER = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef]);

async function fileWith(bytes: Uint8Array): Promise<URL> {
  const dir = await mkdtemp(join(tmpdir(), 'reifydb-wasm-load-'));
  const path = join(dir, 'engine.wasm.gz');
  await writeFile(path, bytes);
  return pathToFileURL(path);
}

describe('wasmBytes', () => {
  it('inflates the shipped gzip file back to the exact wasm bytes', async () => {
    // A lossy or skipped inflate hands the engine garbage and instantiation fails far from the cause.
    const url = await fileWith(gzipSync(WASM_HEADER));
    expect(await wasmBytes(url)).toEqual(WASM_HEADER);
  });

  it('passes through bytes a server already decoded via content-encoding', async () => {
    // A host may serve the .gz with Content-Encoding: gzip, so the body arrives inflated and a second inflate must not run.
    const url = await fileWith(WASM_HEADER);
    expect(await wasmBytes(url)).toEqual(WASM_HEADER);
  });

  it('loads the real shipped engine as a valid wasm module', async () => {
    // The package must carry only the gzipped engine, and it must still compile after inflating.
    const bytes = await wasmBytes();
    expect(Array.from(bytes.slice(0, 4))).toEqual([0x00, 0x61, 0x73, 0x6d]);
    expect(WebAssembly.validate(bytes)).toBe(true);
  });
});

describe('isGzip', () => {
  it('needs both magic bytes', () => {
    // A one-byte or empty body must not be mistaken for gzip, otherwise inflate throws on a truncated file.
    expect(isGzip(new Uint8Array([]))).toBe(false);
    expect(isGzip(new Uint8Array([0x1f]))).toBe(false);
    expect(isGzip(new Uint8Array([0x1f, 0x8b]))).toBe(true);
    expect(isGzip(new Uint8Array([0x8b, 0x1f]))).toBe(false);
  });
});
