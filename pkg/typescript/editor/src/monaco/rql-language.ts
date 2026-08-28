// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { languages } from 'monaco-editor';
import { RQL_KEYWORDS, RQL_LITERALS, RQL_OPERATORS, RQL_TYPES } from '@reifydb/core';

export const rqlLanguageDefinition: languages.IMonarchLanguage = {
  defaultToken: '',
  ignoreCase: true,

  keywords: [...RQL_KEYWORDS],
  constants: [...RQL_LITERALS],
  typeKeywords: [...RQL_TYPES],

  operators: [...RQL_OPERATORS],

  tokenizer: {
    root: [
      // System columns (#rownum, #created_at, #updated_at) — must precede
      // the comment rule so they aren't swallowed as line comments.
      [/#(?:rownum|created_at|updated_at)\b/, 'variable.predefined'],

      // Comments
      [/#.*/, 'comment'],

      // Identifier immediately followed by `::` — first segment of a
      // namespace path (app::users, math::avg, ns::a::b). Must precede
      // the `::` rule and the generic keyword-cases rule so it wins for
      // any identifier, including ones also in `keywords`/`typeKeywords`.
      [/[a-zA-Z_][\w$]*(?=\s*::)/, 'namespace'],

      // Namespace separator (must precede named arguments). Pushes into
      // @nsMember so the identifier that follows is classified
      // structurally (namespace / function / entity) instead of via the
      // flat identifier rule below.
      [/::/, { token: 'operator', next: '@nsMember' }],

      // Named arguments (negative lookahead prevents matching `::`)
      [/(\w+)\s*:(?!:)/, 'key'],

      // Variable references
      [/\$[\w$]+/, 'variable'],

      // Bare call: identifier immediately followed by `(`.
      [
        /[a-zA-Z_][\w$]*(?=\s*\()/,
        {
          cases: {
            '@keywords': 'keyword',
            '@typeKeywords': 'type',
            '@default': 'function',
          },
        },
      ],

      // Identifiers and keywords (case insensitive)
      [
        /[a-zA-Z_][\w$]*/,
        {
          cases: {
            '@keywords': 'keyword',
            '@constants': 'constant',
            '@typeKeywords': 'type',
            '@default': 'identifier',
          },
        },
      ],

      // Whitespace
      { include: '@whitespace' },

      // Brackets
      [/[{}()[\]]/, '@brackets'],

      // Numbers with underscores and scientific notation support
      [/[+-]?(?:[\d_]+(?:\.[\d_]+)?|\.[\d_]+)(?:[eE][+-]?\d+)?/, 'number'],

      // Strings
      [/"([^"\\]|\\.)*$/, 'string.invalid'],
      [/"/, { token: 'string.quote', bracket: '@open', next: '@string' }],

      // Single-quoted strings
      [/'([^'\\]|\\.)*$/, 'string.invalid'],
      [/'/, { token: 'string.quote', bracket: '@open', next: '@singlestring' }],

      // Operators — multi-char first for longest match
      [/<<|>>|\.\./, 'operator'],
      [/==|!=|->|=>|>=|<=|~=|:=/, 'operator'],
      [/&&|\|\||\?\?/, 'operator'],
      [/\/\//, 'operator'],

      // Single-char operators
      [/[+\-*/%|.=<>!&^?]/, 'operator'],

      // Delimiters
      [/[;,]/, 'delimiter'],
    ],

    // Entered right after consuming `::`. Classifies the following
    // identifier as namespace (chained path continues), function (call),
    // or entity (leaf table/view/column-owner name) — always pops back
    // to root.
    nsMember: [
      [/[a-zA-Z_][\w$]*(?=\s*::)/, { token: 'namespace', next: '@pop' }],
      [/[a-zA-Z_][\w$]*(?=\s*\()/, { token: 'function', next: '@pop' }],
      [/[a-zA-Z_][\w$]*/, { token: 'entity', next: '@pop' }],
      [/\s+/, 'white'],
      [/./, { token: '@rematch', next: '@pop' }],
    ],

    string: [
      [/[^\\"]+/, 'string'],
      [/\\./, 'string.escape'],
      [/"/, { token: 'string.quote', bracket: '@close', next: '@pop' }],
    ],

    singlestring: [
      [/[^\\']+/, 'string'],
      [/\\./, 'string.escape'],
      [/'/, { token: 'string.quote', bracket: '@close', next: '@pop' }],
    ],

    whitespace: [
      [/[ \t\r\n]+/, 'white'],
    ],
  },
};

export const rqlLanguageConfiguration: languages.LanguageConfiguration = {
  comments: {
    lineComment: '#',
  },
  brackets: [
    ['{', '}'],
    ['[', ']'],
    ['(', ')'],
  ],
  autoClosingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"', notIn: ['string'] },
    { open: "'", close: "'", notIn: ['string'] },
  ],
  surroundingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
  folding: {
    offSide: true,
  },
};
