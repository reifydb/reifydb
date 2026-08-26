// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
    plugins: [react()],
    resolve: {
        alias: [
            {find: '@', replacement: path.resolve(__dirname, './src')},
        ],
    },
    test: {
        include: ['tests/**/*.test.{ts,tsx}'],
        globals: true,
        environment: 'jsdom',
        setupFiles: ['./vitest.setup.ts'],
        css: false,
        server: {
            deps: {
                inline: [/@reifydb\/ui/, /prismjs/],
            },
        },
    },
})
