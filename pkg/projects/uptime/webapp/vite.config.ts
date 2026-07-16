// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
    plugins: [react()],
    base: '/',
    build: {
        outDir: 'dist',
        assetsDir: 'assets',
        rollupOptions: {
            output: {
                entryFileNames: 'assets/[name].[hash].js',
                chunkFileNames: 'assets/[name].[hash].js',
                assetFileNames: 'assets/[name].[hash].[ext]'
            }
        }
    },
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
            "@reifydb/core": path.resolve(__dirname, "../../../typescript/core/src/index.ts"),
            "@reifydb/client": path.resolve(__dirname, "../../../typescript/client/src/index.ts"),
            "@reifydb/auth": path.resolve(__dirname, "../../../typescript/auth/src/index.ts"),
        },
    },
    server: {
        proxy: {
            '/api': {target: 'http://127.0.0.1:8080', changeOrigin: true},
            '/db': {target: 'http://127.0.0.1:8080', changeOrigin: true},
        },
    },
})
