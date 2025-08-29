import {defineConfig} from 'vitest/config';
import {resolve} from 'path';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        hookTimeout: 10000,
        testTimeout: 15000,

        include: [
            'tests/integration/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
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
            '@tests': resolve(__dirname, './tests')
        }
    },

    esbuild: {
        target: 'es2020'
    }
});
