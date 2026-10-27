// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 30_000,
        hookTimeout: 30_000,
        teardownTimeout: 30_000,
        setupFiles: ['./tests/setup.ts'],

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
