// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
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
    // Concatenate CSS files into dist/styles.css
    const tokens = readFileSync(resolve(__dirname, 'src/styles/tokens.css'), 'utf-8');
    const console_ = readFileSync(resolve(__dirname, 'src/styles/console.css'), 'utf-8');
    // console.css already imports tokens.css, so replace the import with the actual content
    const combined = console_.replace("@import './tokens.css';", tokens);
    mkdirSync(resolve(__dirname, 'dist'), { recursive: true });
    writeFileSync(resolve(__dirname, 'dist/styles.css'), combined);
  },
});
