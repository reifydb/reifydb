// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export const WASM_URL = new URL('../wasm/reifydb_webassembly_bg.wasm.gz', import.meta.url);

export function isGzip(bytes: Uint8Array): boolean {
  return bytes.length > 1 && bytes[0] === 0x1f && bytes[1] === 0x8b;
}

function isNode(): boolean {
  return typeof process !== 'undefined' && process.versions?.node !== undefined;
}

async function inflate(bytes: Uint8Array): Promise<Uint8Array> {
  if (isNode()) {
    const { gunzipSync } = await import('node:zlib');
    return new Uint8Array(gunzipSync(bytes));
  }
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function read(url: URL): Promise<Uint8Array> {
  if (isNode() && url.protocol === 'file:') {
    const [{ readFile }, { fileURLToPath }] = await Promise.all([import('node:fs/promises'), import('node:url')]);
    return new Uint8Array(await readFile(fileURLToPath(url)));
  }
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`failed to fetch ${url}: ${response.status} ${response.statusText}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

export async function wasmBytes(url: URL = WASM_URL): Promise<Uint8Array> {
  const bytes = await read(url);
  return isGzip(bytes) ? inflate(bytes) : bytes;
}
