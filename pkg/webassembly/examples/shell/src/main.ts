// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import init, { WasmDB } from 'reifydb-wasm';
import { Shell, WasmExecutor, COLORS } from '@reifydb/shell';

const C = COLORS;

async function main(): Promise<void> {
  const container = document.getElementById('terminal-container');
  if (!container) {
    console.error('Terminal container not found');
    return;
  }

  try {
    // Initialize WASM
    await init();

    // Create database
    const db = new WasmDB();

    // Create and start shell
    const shell = new Shell(container, {
      executor: new WasmExecutor(db),
      welcomeMessage: [
        '',
        `${C.bold}${C.cyan}ReifyDB Shell${C.reset} ${C.dim}(WebAssembly)${C.reset}`,
        '',
        `Type ${C.green}.help${C.reset} for available commands`,
        `Statements must end with a semicolon ${C.yellow};${C.reset}`,
        '',
      ],
    });
    shell.start();

    // Expose for debugging
    (window as unknown as { db: WasmDB }).db = db;

  } catch (error) {
    console.error('Failed to initialize:', error);
    container.innerHTML = `
      <div class="loading" style="color: #f38ba8;">
        <span>Failed to initialize ReifyDB Shell</span>
        <span style="font-size: 12px; margin-top: 8px; opacity: 0.7;">
          ${error instanceof Error ? error.message : String(error)}
        </span>
      </div>
    `;
  }
}

main();
