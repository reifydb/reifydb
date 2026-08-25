// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {tokenize_rql, type RqlTokenKind} from '@reifydb/core';
import type {Highlighter} from '../types';

export interface RqlStyle {
  color?: string;
  bold?: boolean;
  italic?: boolean;
}

export type RqlPalette = Partial<Record<RqlTokenKind, RqlStyle>>;

export const reifydb_dark_palette: RqlPalette = {
  keyword: {color: '#818cf8', bold: true},
  string: {color: '#a5b4fc'},
  number: {color: '#a5b4fc', bold: true},
  comment: {color: '#71717a', italic: true},
  operator: {color: '#a1a1aa'},
  identifier: {color: '#e4e4e7'},
  key: {color: '#818cf8', bold: true},
  namespace: {color: '#5eead4'},
  entity: {color: '#99f6e4'},
  function: {color: '#fbbf24'},
};

const RESET = '\x1b[0m';

function sgr(style: RqlStyle): string {
  const parameters: string[] = [];
  if (style.bold) parameters.push('1');
  if (style.italic) parameters.push('3');

  if (style.color) {
    const hex = style.color.replace('#', '');
    const value = parseInt(hex, 16);
    parameters.push(`38;2;${(value >> 16) & 0xff};${(value >> 8) & 0xff};${value & 0xff}`);
  }

  return parameters.length === 0 ? '' : `\x1b[${parameters.join(';')}m`;
}

export function rql_highlighter(palette: RqlPalette = reifydb_dark_palette): Highlighter {
  const prefixes = new Map<RqlTokenKind, string>();
  for (const [kind, style] of Object.entries(palette)) {
    if (style) prefixes.set(kind as RqlTokenKind, sgr(style));
  }

  return (line: string, context?: string): string => {
    const offset = context?.length ?? 0;
    const end = offset + line.length;
    let painted = '';

    for (const token of tokenize_rql((context ?? '') + line)) {
      const start = Math.max(token.start, offset);
      const stop = Math.min(token.end, end);
      if (start >= stop) continue;

      const text = line.slice(start - offset, stop - offset);
      const prefix = prefixes.get(token.kind);
      painted += prefix ? prefix + text + RESET : text;
    }

    return painted;
  };
}
