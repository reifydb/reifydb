import {defineConfig} from 'vitest/config';
import {resolve} from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        testTimeout: 1000,
        hookTimeout: 3000,
        teardownTimeout: 5000,

        include: [
            'tests/integration/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
        ],
        exclude: [
            'tests/unit/**',
            'node_modules/**',
            'dist/**'
        ],

        globalSetup: ['./tests/integration/setup.ts'],

        pool: 'forks',
        poolOptions: {
            forks: {
                singleFork: true
            }
        },

        retry: 2,
        reporters: process.env.CI ? ['junit', 'github-actions'] : ['verbose'],
        outputFile: {
            junit: './test-results/integration-junit.xml'
        },

        env: {
            NODE_ENV: 'test',
            REIFYDB_WS_URL: process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:9090',
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
