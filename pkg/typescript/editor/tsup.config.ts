// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { defineConfig } from 'tsup';
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  external: ['react', 'react-dom', 'monaco-editor', '@monaco-editor/react'],
  onSuccess: async () => {
    const tokens = readFileSync(resolve(__dirname, 'src/styles/tokens.css'), 'utf-8');
    const editorCss = readFileSync(resolve(__dirname, 'src/styles/editor.css'), 'utf-8');
    const combined = editorCss.replace("@import './tokens.css';", tokens);
    mkdirSync(resolve(__dirname, 'dist'), { recursive: true });
    writeFileSync(resolve(__dirname, 'dist/styles.css'), combined);
  },
});
