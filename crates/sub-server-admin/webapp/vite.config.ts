import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
    plugins: [react()],
    base: '/',
    build: {
        outDir: 'dist',
        assetsDir: 'assets',
        rollupOptions: {
            output: {
                entryFileNames: 'assets/[name].[hash].js',
                chunkFileNames: 'assets/[name].[hash].js',
                assetFileNames: 'assets/[name].[hash].[ext]'
            }
        }
    },
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
            "@reifydb/core": path.resolve(__dirname, "../../../pkg/typescript/core/src/index.ts"),
            "@reifydb/client": path.resolve(__dirname, "../../../pkg/typescript/client/src/index.ts"),
            "@reifydb/react": path.resolve(__dirname, "../../../pkg/typescript/react/src/index.ts"),
            "@reifydb/shell": path.resolve(__dirname, "../../../pkg/typescript/shell/src/index.ts"),
        },
    },
})