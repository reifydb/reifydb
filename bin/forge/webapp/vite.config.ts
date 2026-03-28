// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { resolve } from 'path'

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  base: '/',
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name].[hash].js',
        chunkFileNames: 'assets/[name].[hash].js',
        assetFileNames: 'assets/[name].[hash].[ext]',
      },
    },
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
      '@reifydb/react': resolve(__dirname, '../../../pkg/typescript/react/src/index.ts'),
      '@reifydb/client': resolve(__dirname, '../../../pkg/typescript/client/src/index.ts'),
      '@reifydb/core': resolve(__dirname, '../../../pkg/typescript/core/src/index.ts'),
    },
  },
})
