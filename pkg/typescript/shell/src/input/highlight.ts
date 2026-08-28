// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {tokenizeRql, type RqlTokenKind} from '@reifydb/core';
import {RESET, sgr, type AnsiStyle} from '../ansi';
import type {Highlighter} from '../types';

export type RqlPalette = Partial<Record<RqlTokenKind, AnsiStyle>>;

export const reifydbDarkPalette: RqlPalette = {
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

export function rqlHighlighter(palette: RqlPalette = reifydbDarkPalette): Highlighter {
  const prefixes = new Map<RqlTokenKind, string>();
  for (const [kind, style] of Object.entries(palette)) {
    if (style) prefixes.set(kind as RqlTokenKind, sgr(style));
  }

  return (line: string, context?: string): string => {
    const offset = context?.length ?? 0;
    const end = offset + line.length;
    let painted = '';

    for (const token of tokenizeRql((context ?? '') + line)) {
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
