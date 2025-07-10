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
        testTimeout: 30000,
        hookTimeout: 10000,
        teardownTimeout: 10000,

        // Test file patterns
        include: [
            'tests/unit/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
            'src/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}'
        ],
        exclude: [
            'tests/integration/**/*',
            'node_modules/**',
            'dist/**',
            '.git/**',
            '.vscode/**'
        ],

        // Coverage configuration
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

        // Setup files
        setupFiles: ['./tests/setup.ts'],

        // Environment variables
        env: {
            NODE_ENV: 'test',
        }
    },

    // Path resolution
    resolve: {
        alias: {
            '@': resolve(__dirname, './src'),
            '@tests': resolve(__dirname, './tests')
        }
    },

    // ESM configuration
    esbuild: {
        target: 'node16'
    }
});
