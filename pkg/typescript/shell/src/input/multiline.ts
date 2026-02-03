// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

export class MultilineBuffer {
  private lines: string[] = [];

  get isEmpty(): boolean {
    return this.lines.length === 0;
  }

  get content(): string {
    return this.lines.join(' ');
  }

  addLine(line: string): void {
    this.lines.push(line);
  }

  clear(): void {
    this.lines = [];
  }

  isComplete(): boolean {
    // A statement is complete when it ends with a semicolon
    const full = this.content.trim();
    return full.endsWith(';');
  }

  static isStatementComplete(input: string): boolean {
    const trimmed = input.trim();
    return trimmed.endsWith(';');
  }
}
