// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

const SDK_ROOT = path.resolve(__dirname, '../../../typescript')
const useLocalSdk = process.env.VITE_LOCAL_SDK === '1'

const reifydbAliases = [
    {find: /^@reifydb\/client$/, replacement: path.join(SDK_ROOT, 'client/src/index.ts')},
    {find: /^@reifydb\/auth$/, replacement: path.join(SDK_ROOT, 'auth/src/index.ts')},
    ...(useLocalSdk
        ? [
            {find: /^@reifydb\/core$/, replacement: path.join(SDK_ROOT, 'core/src/index.ts')},
            {find: /^@reifydb\/ui$/, replacement: path.join(SDK_ROOT, 'ui/src/index.ts')},
            {find: /^@reifydb\/ui\/styles\.css$/, replacement: path.join(SDK_ROOT, 'ui/src/styles/index.css')},
        ]
        : []),
]

export default defineConfig({
    plugins: [react(), tailwindcss()],
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
        alias: [
            ...reifydbAliases,
            {find: '@', replacement: path.resolve(__dirname, './src')},
            {find: 'react', replacement: path.resolve(__dirname, 'node_modules/react')},
            {find: 'react-dom', replacement: path.resolve(__dirname, 'node_modules/react-dom')},
        ],
    },
    server: {
        proxy: {
            '/api': {target: 'http://127.0.0.1:8080', changeOrigin: true},
            '/db': {target: 'http://127.0.0.1:8080', changeOrigin: true},
        },
    },
})
