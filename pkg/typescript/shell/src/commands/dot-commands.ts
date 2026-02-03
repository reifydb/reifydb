// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { TerminalAdapter } from '../terminal/adapter';
import type { DotCommandContext, DotCommandResult, DisplayMode } from '../types';

const C = TerminalAdapter.COLORS;

export async function handleDotCommand(
  input: string,
  context: DotCommandContext
): Promise<DotCommandResult> {
  const parts = input.trim().split(/\s+/);
  const command = parts[0].toLowerCase();
  const args = parts.slice(1);

  switch (command) {
    case '.help':
      showHelp(context.terminal);
      return { handled: true };

    case '.quit':
    case '.exit':
      context.terminal.writeln(`${C.yellow}Goodbye!${C.reset}`);
      context.terminal.writeln('');
      return { handled: true, exit: true };

    case '.clear':
      context.clearScreen();
      return { handled: true };

    case '.mode':
      handleMode(args, context);
      return { handled: true };

    case '.history':
      showHistory(context);
      return { handled: true };

    case '.tables':
      await showTables(context);
      return { handled: true };

    case '.schema':
      await showSchema(args, context);
      return { handled: true };

    default:
      if (command.startsWith('.')) {
        context.terminal.writeln(
          `${C.red}Unknown command: ${command}${C.reset}`
        );
        context.terminal.writeln(`Type ${C.cyan}.help${C.reset} for available commands.`);
        return { handled: true };
      }
      return { handled: false };
  }
}

function showHelp(terminal: TerminalAdapter): void {
  terminal.writeln('');
  terminal.writeln(`${C.bold}${C.cyan}Available commands:${C.reset}`);
  terminal.writeln('');
  terminal.writeln(`  ${C.green}.help${C.reset}              Show this help message`);
  terminal.writeln(`  ${C.green}.quit${C.reset}, ${C.green}.exit${C.reset}      Exit message`);
  terminal.writeln(`  ${C.green}.clear${C.reset}             Clear the screen`);
  terminal.writeln(`  ${C.green}.mode${C.reset} [mode]       Set display mode (truncate|full)`);
  terminal.writeln(`  ${C.green}.history${C.reset}           Show command history`);
  terminal.writeln(`  ${C.green}.tables${C.reset}            List all tables`);
  terminal.writeln(`  ${C.green}.schema${C.reset} [table]    Show table schema`);
  terminal.writeln('');
  terminal.writeln(`${C.bold}${C.cyan}Keyboard shortcuts:${C.reset}`);
  terminal.writeln('');
  terminal.writeln(`  ${C.yellow}Left/Right${C.reset}         Move cursor`);
  terminal.writeln(`  ${C.yellow}Ctrl+Left/Right${C.reset}    Move by word`);
  terminal.writeln(`  ${C.yellow}Home/End${C.reset}           Start/end of line`);
  terminal.writeln(`  ${C.yellow}Ctrl+A/E${C.reset}           Start/end of line`);
  terminal.writeln(`  ${C.yellow}Ctrl+U${C.reset}             Clear line`);
  terminal.writeln(`  ${C.yellow}Ctrl+W${C.reset}             Delete word`);
  terminal.writeln(`  ${C.yellow}Up/Down${C.reset}            Navigate history`);
  terminal.writeln(`  ${C.yellow}Ctrl+C${C.reset}             Cancel input`);
  terminal.writeln('');
  terminal.writeln(`${C.dim}Statements must end with a semicolon (;)${C.reset}`);
  terminal.writeln('');
}

function handleMode(args: string[], context: DotCommandContext): void {
  const terminal = context.terminal;

  if (args.length === 0) {
    terminal.writeln(`Current display mode: ${C.cyan}${context.displayMode}${C.reset}`);
    return;
  }

  const mode = args[0].toLowerCase();
  if (mode === 'truncate' || mode === 'full') {
    context.setDisplayMode(mode as DisplayMode);
    terminal.writeln(`Display mode set to: ${C.cyan}${mode}${C.reset}`);
  } else {
    terminal.writeln(
      `${C.red}Unknown mode: ${mode}${C.reset}. Use 'truncate' or 'full'.`
    );
  }
}

function showHistory(context: DotCommandContext): void {
  const terminal = context.terminal;
  const entries = context.history.getAll();

  if (entries.length === 0) {
    terminal.writeln(`${C.dim}No command history${C.reset}`);
    return;
  }

  terminal.writeln('');
  terminal.writeln(`${C.bold}${C.cyan}Command history:${C.reset}`);
  terminal.writeln('');

  // Show last 20 entries
  const toShow = entries.slice(-20);
  const startIdx = entries.length - toShow.length;

  toShow.forEach((entry, i) => {
    const num = String(startIdx + i + 1).padStart(4, ' ');
    terminal.writeln(`${C.dim}${num}${C.reset}  ${entry}`);
  });

  if (entries.length > 20) {
    terminal.writeln('');
    terminal.writeln(`${C.dim}... and ${entries.length - 20} more entries${C.reset}`);
  }
  terminal.writeln('');
}

async function showTables(context: DotCommandContext): Promise<void> {
  const terminal = context.terminal;

  if (!context.executor.getTables) {
    terminal.writeln(`${C.dim}.tables command not supported by this executor${C.reset}`);
    return;
  }

  const tables = await context.executor.getTables();

  if (tables.length === 0) {
    terminal.writeln(`${C.dim}No tables found${C.reset}`);
    return;
  }

  terminal.writeln('');
  terminal.writeln(`${C.bold}${C.cyan}Tables:${C.reset}`);
  terminal.writeln('');

  tables.forEach((table) => {
    terminal.writeln(`  ${C.green}${table}${C.reset}`);
  });
  terminal.writeln('');
}

async function showSchema(args: string[], context: DotCommandContext): Promise<void> {
  const terminal = context.terminal;

  if (!context.executor.getSchema) {
    terminal.writeln(`${C.dim}.schema command not supported by this executor${C.reset}`);
    return;
  }

  if (args.length === 0) {
    terminal.writeln(`${C.red}Usage: .schema <table_name>${C.reset}`);
    return;
  }

  const tableName = args[0];
  const schema = await context.executor.getSchema(tableName);

  if (schema) {
    terminal.writeln('');
    terminal.writeln(schema);
    terminal.writeln('');
  } else {
    terminal.writeln(`${C.red}Table not found: ${tableName}${C.reset}`);
  }
}
