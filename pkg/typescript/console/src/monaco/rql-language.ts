// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { languages } from 'monaco-editor';

const KEYWORDS = [
  // Query transforms
  'map', 'extend', 'by', 'from', 'where', 'aggregate', 'having',
  'sort', 'distinct', 'take', 'offset',

  // Joins & set operations
  'left', 'inner', 'natural', 'join', 'on', 'using', 'intersect', 'except',

  // DML
  'insert', 'into', 'update', 'set', 'delete',

  // Control flow
  'let', 'if', 'else', 'end', 'loop', 'while', 'break', 'continue', 'return',

  // Functions & casting
  'fun', 'call', 'apply', 'cast',

  // DDL & schema commands
  'describe', 'show', 'create', 'alter', 'drop', 'filter', 'gate', 'flow', 'window',

  // Operators & predicates
  'in', 'between', 'like', 'is', 'with',

  // Object types
  'namespace', 'sequence', 'series', 'subscription', 'table', 'ringbuffer',
  'column', 'policy', 'property', 'view', 'deferred', 'transactional',

  // Index & constraints
  'index', 'unique', 'primary', 'key', 'asc', 'desc', 'auto', 'increment', 'value',

  // Misc operations
  'exists', 'replace', 'cascade', 'restrict', 'to', 'pause', 'resume',
  'query', 'rename', 'rownum', 'dictionary', 'for', 'output', 'append',
  'assert', 'patch',

  // Enums & pattern matching
  'enum', 'match',

  // Procedures & events
  'procedure', 'event', 'handler', 'dispatch', 'tag',

  // Testing
  'test', 'tests', 'run',

  // Access control
  'user', 'role', 'grant', 'revoke', 'password', 'require', 'execute',
  'access', 'subscribe', 'enable', 'disable',

  // System objects
  'function', 'session', 'feature',

  // Migrations
  'add', 'migration', 'migrate', 'rollback', 'diff', 'version', 'current', 'pending',

  // Misc
  'authentication', 'contains', 'remote', 'error',

  // Additional keywords (not in keyword.rs but valid RQL constructs)
  'derive', 'group', 'union', 'as',
];

const MODULES = ['date', 'math', 'text'];
const BUILTIN_FUNCTIONS = ['case'];
const LITERALS = ['none', 'true', 'false'];

export const rqlLanguageDefinition: languages.IMonarchLanguage = {
  defaultToken: 'invalid',
  ignoreCase: true,

  keywords: [...KEYWORDS, ...MODULES, ...BUILTIN_FUNCTIONS, ...LITERALS],

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
