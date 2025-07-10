import {defineConfig} from 'vitest/config';
import {resolve} from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 1000,
        hookTimeout: 3000,
        teardownTimeout: 5000,

        // Only integration tests
        include: [
            'tests/integration/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'tests/unit/**',
            'node_modules/**',
            'dist/**'
        ],

        // Setup files for each test file
        globalSetup: ['./tests/integration/setup.ts'],

        // Sequential execution for integration tests (avoid conflicts)
        pool: 'forks',
        poolOptions: {
            forks: {
                singleFork: true
            }
        },

        // Retry flaky integration tests
        retry: 2,

        // Reporter for integration tests
        reporters: process.env.CI ? ['junit', 'github-actions'] : ['verbose'],
        outputFile: {
            junit: './test-results/integration-junit.xml'
        },

        // Environment variables for integration tests
        env: {
            NODE_ENV: 'test',
            REIFYDB_WS_URL: process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:9090',
            REIFYDB_TOKEN: process.env.REIFYDB_TOKEN || 'test-token',
            REIFYDB_LOG_LEVEL: 'info'
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
