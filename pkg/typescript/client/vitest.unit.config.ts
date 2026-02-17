// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {defineConfig} from 'vitest/config';
import {resolve} from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 100,
        hookTimeout: 100,
        teardownTimeout: 100,

        include: [
            'tests/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'node_modules/**',
            'dist/**',
            'tests/integration/**',
        ],

        reporters: process.env.CI ? ['junit', 'github-actions'] : ['verbose'],
        outputFile: {
            junit: './test-results/unit-junit.xml'
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

    esbuild: {
        target: 'es2020'
    }
});
