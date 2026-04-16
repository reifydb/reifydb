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

  // DDL & shape commands
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

  // Word operators
  'and', 'or', 'not', 'xor',
];

const MODULES = ['date', 'math', 'text'];
const BUILTIN_FUNCTIONS = ['case'];
const LITERALS = ['none', 'true', 'false'];

const TYPE_KEYWORDS = [
  // Signed integers
  'int1', 'int2', 'int4', 'int8', 'int16', 'int',
  // Unsigned integers
  'uint1', 'uint2', 'uint4', 'uint8', 'uint16', 'uint',
  // Floating point
  'float4', 'float8',
  // Text & binary
  'utf8', 'blob',
  // Boolean
  'bool', 'boolean',
  // Numeric
  'decimal',
  // Temporal
  'date', 'datetime', 'time', 'duration', 'interval',
  // Identifiers & UUIDs
  'uuid4', 'uuid7', 'identityid', 'identity_id', 'dictionaryid', 'dictionary_id',
  // Special
  'any',
  // Container types
  'Option', 'List', 'Record', 'Tuple',
  // Alias
  'text',
];

export const rql_language_definition: languages.IMonarchLanguage = {
  defaultToken: '',
  ignoreCase: true,

  keywords: [...KEYWORDS, ...MODULES, ...BUILTIN_FUNCTIONS],
  constants: LITERALS,
  typeKeywords: TYPE_KEYWORDS,

  operators: ['+', '-', '*', '/', '//', '%', '=', '==', '!=', '->', '=>', '>', '<', '>=', '<=', '~=', '&&', '||', '??'],

  tokenizer: {
    root: [
      // Comments
      [/#.*/, 'comment'],

      // Namespace separator (must precede named arguments)
      [/::/, 'operator'],

      // Named arguments (negative lookahead prevents matching `::`)
      [/(\w+)\s*:(?!:)/, 'key'],

      // Variable references
      [/\$[\w$]+/, 'variable'],

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

export const rql_language_configuration: languages.LanguageConfiguration = {
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
