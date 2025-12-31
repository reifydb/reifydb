// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {defineConfig} from 'vitest/config';
import {resolve} from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'happy-dom',
        hookTimeout: 10000,
        testTimeout: 15000,
        teardownTimeout: 1000,

        include: [
            'tests/integration/**/*.{test,spec,tap}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'node_modules/**',
            'dist/**',
        ],

        pool: 'threads',
        poolOptions: {
            threads: {
                singleThread: false
            }
        },

        retry: 2,
        reporters: process.env.CI
            ? ['verbose', 'github-actions', 'junit']
            : ['verbose'],
        outputFile: {
            junit: './test-results/integration-junit.xml'
        },

        env: {
            NODE_ENV: 'test',
            REIFYDB_WS_URL: process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090',
        }
    },

    resolve: {
        alias: {
            '@': resolve(__dirname, './src'),
            '@tests': resolve(__dirname, './tests'),
            '@reifydb/react': resolve(__dirname, './src/index.ts')
        }
    },

    esbuild: {
        target: 'es2020'
    }
});