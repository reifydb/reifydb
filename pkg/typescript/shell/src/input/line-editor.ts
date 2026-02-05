// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { TerminalAdapter } from '../terminal/adapter';

export class LineEditor {
  private buffer: string = '';
  private cursorPos: number = 0;
  private terminal: TerminalAdapter;

  constructor(terminal: TerminalAdapter) {
    this.terminal = terminal;
  }

  get value(): string {
    return this.buffer;
  }

  get cursor(): number {
    return this.cursorPos;
  }

  clear(): void {
    this.buffer = '';
    this.cursorPos = 0;
  }

  setValue(value: string): void {
    this.buffer = value;
    this.cursorPos = value.length;
  }

  insert(char: string): void {
    this.buffer =
      this.buffer.slice(0, this.cursorPos) +
      char +
      this.buffer.slice(this.cursorPos);
    this.cursorPos++;
  }

  backspace(): boolean {
    if (this.cursorPos > 0) {
      this.buffer =
        this.buffer.slice(0, this.cursorPos - 1) +
        this.buffer.slice(this.cursorPos);
      this.cursorPos--;
      return true;
    }
    return false;
  }

  delete(): boolean {
    if (this.cursorPos < this.buffer.length) {
      this.buffer =
        this.buffer.slice(0, this.cursorPos) +
        this.buffer.slice(this.cursorPos + 1);
      return true;
    }
    return false;
  }

  moveLeft(): boolean {
    if (this.cursorPos > 0) {
      this.cursorPos--;
      this.terminal.write(TerminalAdapter.cursorBack());
      return true;
    }
    return false;
  }

  moveRight(): boolean {
    if (this.cursorPos < this.buffer.length) {
      this.cursorPos++;
      this.terminal.write(TerminalAdapter.cursorForward());
      return true;
    }
    return false;
  }

  moveToStart(): void {
    if (this.cursorPos > 0) {
      this.terminal.write(TerminalAdapter.cursorBack(this.cursorPos));
      this.cursorPos = 0;
    }
  }

  moveToEnd(): void {
    if (this.cursorPos < this.buffer.length) {
      const distance = this.buffer.length - this.cursorPos;
      this.terminal.write(TerminalAdapter.cursorForward(distance));
      this.cursorPos = this.buffer.length;
    }
  }

  moveWordLeft(): void {
    if (this.cursorPos === 0) return;

    let newPos = this.cursorPos - 1;

    // Skip whitespace
    while (newPos > 0 && /\s/.test(this.buffer[newPos])) {
      newPos--;
    }

    // Move to start of word
    while (newPos > 0 && !/\s/.test(this.buffer[newPos - 1])) {
      newPos--;
    }

    const distance = this.cursorPos - newPos;
    if (distance > 0) {
      this.terminal.write(TerminalAdapter.cursorBack(distance));
      this.cursorPos = newPos;
    }
  }

  moveWordRight(): void {
    if (this.cursorPos >= this.buffer.length) return;

    let newPos = this.cursorPos;

    // Move past current word
    while (newPos < this.buffer.length && !/\s/.test(this.buffer[newPos])) {
      newPos++;
    }

    // Skip whitespace
    while (newPos < this.buffer.length && /\s/.test(this.buffer[newPos])) {
      newPos++;
    }

    const distance = newPos - this.cursorPos;
    if (distance > 0) {
      this.terminal.write(TerminalAdapter.cursorForward(distance));
      this.cursorPos = newPos;
    }
  }

  clearLine(): void {
    this.buffer = '';
    this.cursorPos = 0;
  }

  deleteToEnd(): void {
    this.buffer = this.buffer.slice(0, this.cursorPos);
  }

  deleteToStart(): void {
    this.buffer = this.buffer.slice(this.cursorPos);
    this.cursorPos = 0;
  }

  deleteWord(): void {
    if (this.cursorPos === 0) return;

    let newPos = this.cursorPos - 1;

    // Skip whitespace
    while (newPos > 0 && /\s/.test(this.buffer[newPos])) {
      newPos--;
    }

    // Move to start of word
    while (newPos > 0 && !/\s/.test(this.buffer[newPos - 1])) {
      newPos--;
    }

    this.buffer = this.buffer.slice(0, newPos) + this.buffer.slice(this.cursorPos);
    this.cursorPos = newPos;
  }

  // Called by shell to render the line with prompt
  render(prompt: string): void {
    this.terminal.write(
      '\r' +
      TerminalAdapter.clearToEndOfLine() +
      prompt +
      this.buffer
    );

    // Position cursor correctly
    const cursorOffset = this.buffer.length - this.cursorPos;
    if (cursorOffset > 0) {
      this.terminal.write(TerminalAdapter.cursorBack(cursorOffset));
    }
  }
}
