// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { languages } from 'monaco-editor';

const TRANSFORMS = [
  'aggregate',
  'append',
  'create',
  'derive',
  'deferred',
  'distinct',
  'extend',
  'filter',
  'from',
  'group',
  'insert',
  'join',
  'map',
  'sort',
  'take',
  'union',
  'view',
  'with',
];

const MODULES = ['date', 'math', 'text'];
const BUILTIN_FUNCTIONS = ['case'];
const KEYWORDS = ['set', 'as', 'on', 'by'];
const LITERALS = ['none', 'true', 'false'];

export const rqlLanguageDefinition: languages.IMonarchLanguage = {
  defaultToken: 'invalid',
  ignoreCase: true,

  keywords: [...TRANSFORMS, ...MODULES, ...BUILTIN_FUNCTIONS, ...KEYWORDS, ...LITERALS],

  operators: ['+', '-', '*', '/', '//', '%', '=', '==', '!=', '->', '=>', '>', '<', '>=', '<=', '~=', '&&', '||', '??'],

  tokenizer: {
    root: [
      // Comments
      [/#.*/, 'comment'],

      // Named arguments
      [/(\w+)\s*:/, 'key'],

      // Identifiers and keywords (case insensitive)
      [
        /[a-zA-Z_$][\w$]*/,
        {
          cases: {
            '@keywords': 'keyword',
            '@default': 'identifier',
          },
        },
      ],

      // Whitespace
      { include: '@whitespace' },

      // Brackets
      [/[{}()[\]]/, '@brackets'],

      // Numbers with underscores support
      [/[+-]?(?:[\d_]+(?:\.[\d_]+)?|\.[\d_]+)/, 'number'],

      // Strings
      [/"([^"\\]|\\.)*$/, 'string.invalid'],
      [/"/, { token: 'string.quote', bracket: '@open', next: '@string' }],

      // Single-quoted strings
      [/'([^'\\]|\\.)*$/, 'string.invalid'],
      [/'/, { token: 'string.quote', bracket: '@open', next: '@singlestring' }],

      // Operators
      [/[+\-*/%]/, 'operator'],
      [/\/\//, 'operator'],
      [/==|!=|->|=>|>=|<=|~=|>|</, 'operator'],
      [/&&|\|\||\?\?/, 'operator'],
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
      [/\/\*/, 'comment', '@comment'],
      [/\/\/.*$/, 'comment'],
    ],

    comment: [
      [/[^/*]+/, 'comment'],
      [/\/\*/, 'comment', '@push'],
      [/\*\//, 'comment', '@pop'],
      [/[/*]/, 'comment'],
    ],
  },
};

export const rqlLanguageConfiguration: languages.LanguageConfiguration = {
  comments: {
    lineComment: '#',
    blockComment: ['/*', '*/'],
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
