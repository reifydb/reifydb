// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  external: [/\.\.\/wasm\/.*/],
});
