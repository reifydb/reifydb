/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 100,
        hookTimeout: 100,
        teardownTimeout: 100,

        include: [
            'tests/unit/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'tests/integration/**/*',
            'node_modules/**',
            'dist/**',
            '.git/**',
            '.vscode/**'
        ],

        coverage: {
            provider: 'v8',
            reporter: ['text', 'json', 'html', 'lcov'],
            reportsDirectory: './coverage',
            exclude: [
                'tests/**',
                'dist/**',
                'node_modules/**',
                'src/**/*.d.ts',
                'src/**/*.test.ts',
                'src/**/*.spec.ts',
                'vitest.config.ts',
                'vitest.*.config.ts'
            ],
            thresholds: {
                global: {
                    branches: 80,
                    functions: 80,
                    lines: 80,
                    statements: 80
                }
            }
        },

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

    esbuild: {
        target: 'node16'
    }
});
