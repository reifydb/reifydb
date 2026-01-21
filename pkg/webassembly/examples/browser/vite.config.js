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
