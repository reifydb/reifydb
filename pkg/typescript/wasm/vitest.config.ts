// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { defineConfig } from 'vitest/config';
import { resolve } from 'path';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';

export default defineConfig({
    plugins: [wasm(), topLevelAwait()],
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 30_000,
        hookTimeout: 30_000,
        teardownTimeout: 30_000,

        include: [
            'tests/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'node_modules/**',
            'dist/**',
            '.git/**',
        ],

        reporters: process.env.CI ? ['junit', 'github-actions'] : ['verbose'],
        outputFile: {
            junit: './test-results/junit.xml'
        },

        env: {
            NODE_ENV: 'test',
        }
    },

    resolve: {
        alias: {
            '@': resolve(__dirname, './src'),
            '@tests': resolve(__dirname, './tests')
        }
    },
});
