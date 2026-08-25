// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export interface AnsiStyle {
  color?: string;
  bold?: boolean;
  italic?: boolean;
  dim?: boolean;
}

export const RESET = '\x1b[0m';

export function sgr(style: AnsiStyle): string {
  const parameters: string[] = [];
  if (style.bold) parameters.push('1');
  if (style.dim) parameters.push('2');
  if (style.italic) parameters.push('3');

  if (style.color) {
    const hex = style.color.replace('#', '');
    const value = parseInt(hex, 16);
    parameters.push(`38;2;${(value >> 16) & 0xff};${(value >> 8) & 0xff};${value & 0xff}`);
  }

  return parameters.length === 0 ? '' : `\x1b[${parameters.join(';')}m`;
}

export function paint(text: string, prefix: string): string {
  return prefix === '' ? text : prefix + text + RESET;
}
