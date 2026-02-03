// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  resolve: {
    alias: {
      'reifydb-wasm': resolve(__dirname, '../../dist/web/reifydb_webassembly.js')
    }
  },
  server: {
    fs: {
      allow: [
        '.',
        resolve(__dirname, '../../dist')
      ]
    }
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: ['reifydb-wasm']
  },
  build: {
    target: 'ES2022'
  }
});
