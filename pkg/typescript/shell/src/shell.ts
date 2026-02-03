// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { TerminalAdapter } from './terminal/adapter';
import { COLORS } from './terminal/theme';
import { LineEditor } from './input/line-editor';
import { CommandHistory } from './input/history';
import { MultilineBuffer } from './input/multiline';
import { handleDotCommand } from './commands/dot-commands';
import { OutputFormatter } from './output/formatter';
import type { ShellOptions, DisplayMode, Executor } from './types';

const C = COLORS;

// Default prompts
const DEFAULT_PRIMARY_PROMPT = `${C.cyan}reifydb${C.reset}${C.brightBlack}>${C.reset} `;
const DEFAULT_CONTINUATION_PROMPT = `${C.brightBlack}     ...${C.reset} `;

// Prompt lengths without ANSI codes (for cursor positioning)
const DEFAULT_PRIMARY_PROMPT_LEN = 9;  // "reifydb> "
const DEFAULT_CONTINUATION_PROMPT_LEN = 8;  // "     ... "

export class Shell {
  private terminal: TerminalAdapter;
  private lineEditor: LineEditor;
  private history: CommandHistory;
  private multiline: MultilineBuffer;
  private executor: Executor;
  private formatter: OutputFormatter;
  private displayMode: DisplayMode;
  private isExited: boolean = false;

  // Configurable options
  private primaryPrompt: string;
  private primaryPromptLen: number;
  private continuationPrompt: string;
  private continuationPromptLen: number;
  private welcomeMessage: string | string[] | (() => string[]) | undefined;
  private onExit: (() => void) | undefined;
  private onFullscreenChange: ((isFullscreen: boolean) => void) | undefined;

  constructor(container: HTMLElement, options: ShellOptions) {
    this.executor = options.executor;
    this.displayMode = options.displayMode ?? 'full';
    this.welcomeMessage = options.welcomeMessage;
    this.onExit = options.onExit;
    this.onFullscreenChange = options.onFullscreenChange;

    // Set up prompts
    this.primaryPrompt = options.prompt ?? DEFAULT_PRIMARY_PROMPT;
    this.primaryPromptLen = options.promptLength ?? DEFAULT_PRIMARY_PROMPT_LEN;
    this.continuationPrompt = options.continuationPrompt ?? DEFAULT_CONTINUATION_PROMPT;
    this.continuationPromptLen = options.continuationPromptLength ?? DEFAULT_CONTINUATION_PROMPT_LEN;

    // Initialize components
    this.terminal = new TerminalAdapter(container, options.theme);
    this.lineEditor = new LineEditor(this.terminal);
    this.history = new CommandHistory(options.historyStorage, options.historyKey);
    this.multiline = new MultilineBuffer();
    this.formatter = new OutputFormatter(this.terminal, this.displayMode);

    this.setupKeyHandler();
  }

  start(): void {
    this.showWelcomeBanner();
    this.showPrompt();
    this.terminal.focus();
  }

  dispose(): void {
    this.isExited = true;
    this.terminal.exitFullscreen();
    this.terminal.dispose();
  }

  get isFullscreen(): boolean {
    return this.terminal.isFullscreen;
  }

  enterFullscreen(): void {
    this.terminal.enterFullscreen();
    this.onFullscreenChange?.(true);
  }

  exitFullscreen(): void {
    this.terminal.exitFullscreen();
    this.onFullscreenChange?.(false);
  }

  private showWelcomeBanner(): void {
    if (this.welcomeMessage === undefined) {
      // Default welcome banner
      this.terminal.writeln('');
      this.terminal.writeln(`${C.bold}${C.cyan}ReifyDB Shell${C.reset}`);
      this.terminal.writeln('');
      this.terminal.writeln(`Type ${C.green}.help${C.reset} for available commands`);
      this.terminal.writeln(`Statements must end with a semicolon ${C.yellow};${C.reset}`);
      this.terminal.writeln('');
      return;
    }

    let lines: string[];
    if (typeof this.welcomeMessage === 'function') {
      lines = this.welcomeMessage();
    } else if (Array.isArray(this.welcomeMessage)) {
      lines = this.welcomeMessage;
    } else {
      lines = [this.welcomeMessage];
    }

    for (const line of lines) {
      this.terminal.writeln(line);
    }
  }

  private showPrompt(): void {
    const prompt = this.multiline.isEmpty ? this.primaryPrompt : this.continuationPrompt;
    this.terminal.write(prompt);
  }

  private getCurrentPromptLen(): number {
    return this.multiline.isEmpty ? this.primaryPromptLen : this.continuationPromptLen;
  }

  private setupKeyHandler(): void {
    this.terminal.onKey((key, event) => {
      if (this.isExited) return;
      this.handleKey(key, event);
    });
  }

  private handleKey(key: string, event: KeyboardEvent): void {
    const code = event.keyCode;

    // Handle Ctrl key combinations
    if (event.ctrlKey) {
      switch (event.key.toLowerCase()) {
        case 'a': // Ctrl+A - go to start
          event.preventDefault();
          this.lineEditor.moveToStart();
          return;

        case 'e': // Ctrl+E - go to end
          event.preventDefault();
          this.lineEditor.moveToEnd();
          return;

        case 'u': // Ctrl+U - clear line
          event.preventDefault();
          this.lineEditor.clearLine();
          this.redrawLine();
          return;

        case 'w': // Ctrl+W - delete word
          event.preventDefault();
          this.lineEditor.deleteWord();
          this.redrawLine();
          return;

        case 'c': // Ctrl+C - cancel
          event.preventDefault();
          this.terminal.writeln('^C');
          this.lineEditor.clear();
          this.multiline.clear();
          this.history.reset();
          this.showPrompt();
          return;

        case 'l': // Ctrl+L - clear screen
          event.preventDefault();
          this.clearScreen();
          return;
      }

      // Ctrl+Left/Right for word navigation
      if (code === 37) { // Left
        event.preventDefault();
        this.lineEditor.moveWordLeft();
        return;
      }
      if (code === 39) { // Right
        event.preventDefault();
        this.lineEditor.moveWordRight();
        return;
      }

      return;
    }

    // Handle special keys
    switch (code) {
      case 13: // Enter
        this.handleEnter();
        return;

      case 8: // Backspace
        if (this.lineEditor.backspace()) {
          this.redrawLine();
        }
        return;

      case 46: // Delete
        if (this.lineEditor.delete()) {
          this.redrawLine();
        }
        return;

      case 37: // Left arrow
        this.lineEditor.moveLeft();
        return;

      case 39: // Right arrow
        this.lineEditor.moveRight();
        return;

      case 38: // Up arrow
        this.navigateHistory('up');
        return;

      case 40: // Down arrow
        this.navigateHistory('down');
        return;

      case 36: // Home
        this.lineEditor.moveToStart();
        return;

      case 35: // End
        this.lineEditor.moveToEnd();
        return;

      case 9: // Tab - ignore for now
        return;

      case 27: // Escape - exit fullscreen
        if (this.terminal.isFullscreen) {
          this.exitFullscreen();
        }
        return;
    }

    // Regular character input
    if (key.length === 1 && !event.ctrlKey && !event.altKey && !event.metaKey) {
      this.lineEditor.insert(key);
      this.redrawLine();
    }
  }

  private redrawLine(): void {
    const prompt = this.multiline.isEmpty ? this.primaryPrompt : this.continuationPrompt;
    this.lineEditor.render(prompt);
  }

  private navigateHistory(direction: 'up' | 'down'): void {
    let entry: string | null;

    if (direction === 'up') {
      entry = this.history.previous(this.lineEditor.value);
    } else {
      entry = this.history.next();
    }

    if (entry !== null) {
      this.lineEditor.setValue(entry);
      this.redrawLine();
    }
  }

  private async handleEnter(): Promise<void> {
    const line = this.lineEditor.value;
    this.terminal.writeln('');
    this.lineEditor.clear();

    // Handle dot commands (only on first line)
    if (this.multiline.isEmpty && line.trim().startsWith('.')) {
      this.history.add(line);
      const result = await handleDotCommand(line, {
        terminal: this.terminal,
        executor: this.executor,
        history: this.history,
        displayMode: this.displayMode,
        setDisplayMode: (mode) => {
          this.displayMode = mode;
          this.formatter.setDisplayMode(mode);
        },
        clearScreen: () => this.clearScreen(),
        isFullscreen: this.isFullscreen,
        enterFullscreen: () => this.enterFullscreen(),
        exitFullscreen: () => this.exitFullscreen(),
      });

      if (result.exit) {
        this.isExited = true;
        if (this.onExit) {
          this.onExit();
        }
        return;
      }

      this.showPrompt();
      return;
    }

    // Add line to multiline buffer
    this.multiline.addLine(line);

    // Check if statement is complete
    if (this.multiline.isComplete()) {
      const statement = this.multiline.content;
      this.multiline.clear();
      this.history.add(statement);
      this.history.reset();

      // Execute the statement
      const result = await this.executor.execute(statement);
      this.formatter.formatResult(result);
    }

    this.showPrompt();
  }

  private clearScreen(): void {
    this.terminal.write(TerminalAdapter.clearScreen());
    this.showPrompt();
    this.terminal.write(this.lineEditor.value);
  }
}
