// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { SnippetError } from '../src/components/snippet-error';

const flat = [
  'Error AST_005',
  '  Unexpected token: expected Operator(CloseCurly), got && near RQL',
  '',
  'LOCATION',
  '  line 1, column 42',
  '',
  'RQL',
  '  1 │ from app::users filter { role == "admin" && !is::none(nickname) }',
  '    │                                          ~~',
  '    │',
  '    │                                       found `&&`',
  '',
  'HELP',
  '  Use Operator(CloseCurly) instead',
  '',
].join('\n');

const nested = [
  'Error CA_004: cannot apply operator',
  '  at "x" (line 1, column 5)',
  '',
  '  ↳ Error AST_005: Unexpected token',
  '    help: Use Operator(CloseCurly) instead',
  '',
].join('\n');

function render(message: string): string {
  return renderToStaticMarkup(createElement(SnippetError, { message }));
}

function plain(html: string): string {
  return html
    .replace(/<[^>]+>/g, '')
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function linesWith(html: string, cls: string): string[] {
  const pattern = new RegExp(`<span class="${cls}">([^<]*)</span>`, 'g');
  return [...html.matchAll(pattern)].map((m) => plain(m[1]).replace(/\n$/, ''));
}

describe('snippet error block', () => {
  it('keeps the engine text exactly', () => {
    // the engine renderer is the single source of the layout; any dropped or merged line breaks the underline alignment
    expect(plain(render(flat))).toBe(flat.replace(/\n+$/, ''));
  });

  it('colors the title line', () => {
    // the title is how a reader spots which error fired
    expect(linesWith(render(flat), 'rdb-snippet-error__title')).toEqual(['Error AST_005']);
  });

  it('colors nothing but the error titles', () => {
    // a colored source line or heading pulls the eye off the failing token
    const html = render(flat);
    expect(html.match(/class="/g)).toHaveLength(2);
    expect(linesWith(html, 'rdb-snippet-error__title')).toEqual(['Error AST_005']);
  });

  it('colors every error in a nested cause chain', () => {
    // nested causes come from a different engine renderer; missing the arrow prefix leaves the cause looking like text
    expect(linesWith(render(nested), 'rdb-snippet-error__title')).toEqual([
      'Error CA_004: cannot apply operator',
      '  ↳ Error AST_005: Unexpected token',
    ]);
  });

  it('leaves a plain message uncolored', () => {
    // a plain thrown error is not a diagnostic and must not look like one
    const html = render('network down');

    expect(plain(html)).toBe('network down');
    expect(html).not.toMatch(/rdb-snippet-error__(title|heading|marker)/);
  });
});
