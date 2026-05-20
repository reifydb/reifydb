// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
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
    exclude: ['reifydb_webassembly']
  }
});
