// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
  'returning',

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
  // Vector
  'vector',
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

  keywords: KEYWORDS,
  constants: LITERALS,
  typeKeywords: TYPE_KEYWORDS,

  operators: ['+', '-', '*', '/', '//', '%', '=', '==', '!=', '->', '=>', '>', '<', '>=', '<=', '~=', '&&', '||', '??'],

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
