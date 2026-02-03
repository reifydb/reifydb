// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import './styles.css';
import { defaultTheme, COLORS, type TerminalTheme } from './theme';

export type KeyHandler = (key: string, domEvent: KeyboardEvent) => void;

export class TerminalAdapter {
  private terminal: Terminal;
  private fitAddon: FitAddon;
  private container: HTMLElement;
  private keyHandler: KeyHandler | null = null;
  private resizeObserver: ResizeObserver | null = null;
  private _isFullscreen: boolean = false;

  constructor(container: HTMLElement, theme: TerminalTheme = defaultTheme) {
    this.container = container;
    this.terminal = new Terminal({
      theme,
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Consolas', 'Monaco', monospace",
      fontSize: 14,
      lineHeight: 1.2,
      cursorBlink: true,
      cursorStyle: 'block',
      scrollback: 10000,
      allowProposedApi: true,
    });

    this.fitAddon = new FitAddon();
    this.terminal.loadAddon(this.fitAddon);

    container.innerHTML = '';
    this.terminal.open(container);
    this.fitAddon.fit();

    // Handle resize
    this.resizeObserver = new ResizeObserver(() => {
      this.fitAddon.fit();
    });
    this.resizeObserver.observe(container);

    // Handle key input
    this.terminal.onKey(({ key, domEvent }) => {
      if (this.keyHandler) {
        this.keyHandler(key, domEvent);
      }
    });
  }

  onKey(handler: KeyHandler): void {
    this.keyHandler = handler;
  }

  write(text: string): void {
    this.terminal.write(text);
  }

  writeln(text: string): void {
    this.terminal.writeln(text);
  }

  clear(): void {
    this.terminal.clear();
  }

  get cols(): number {
    return this.terminal.cols;
  }

  get rows(): number {
    return this.terminal.rows;
  }

  focus(): void {
    this.terminal.focus();
  }

  dispose(): void {
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    }
    this.terminal.dispose();
  }

  get isFullscreen(): boolean {
    return this._isFullscreen;
  }

  enterFullscreen(): void {
    if (this._isFullscreen) return;
    this._isFullscreen = true;
    this.container.classList.add('reifydb-shell-fullscreen');
    this.fitAddon.fit();
  }

  exitFullscreen(): void {
    if (!this._isFullscreen) return;
    this._isFullscreen = false;
    this.container.classList.remove('reifydb-shell-fullscreen');
    this.fitAddon.fit();
  }

  // ANSI escape code helpers - static for use without instance
  static readonly COLORS = COLORS;

  // Cursor control helpers
  static cursorUp(n: number = 1): string {
    return `\x1b[${n}A`;
  }

  static cursorDown(n: number = 1): string {
    return `\x1b[${n}B`;
  }

  static cursorForward(n: number = 1): string {
    return `\x1b[${n}C`;
  }

  static cursorBack(n: number = 1): string {
    return `\x1b[${n}D`;
  }

  static cursorPosition(row: number, col: number): string {
    return `\x1b[${row};${col}H`;
  }

  static clearLine(): string {
    return '\x1b[2K';
  }

  static clearToEndOfLine(): string {
    return '\x1b[K';
  }

  static clearScreen(): string {
    return '\x1b[2J\x1b[3J\x1b[H';
  }

  static saveCursor(): string {
    return '\x1b[s';
  }

  static restoreCursor(): string {
    return '\x1b[u';
  }
}
