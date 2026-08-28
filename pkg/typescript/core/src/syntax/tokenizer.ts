// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {RQL_KEYWORDS, RQL_LITERALS, RQL_TYPES} from './keyword';

export type RqlTokenKind =
    | 'keyword'
    | 'type'
    | 'constant'
    | 'identifier'
    | 'namespace'
    | 'entity'
    | 'function'
    | 'key'
    | 'variable'
    | 'variable.predefined'
    | 'comment'
    | 'string'
    | 'number'
    | 'operator'
    | 'delimiter'
    | 'bracket'
    | 'white';

export interface RqlToken {
    kind: RqlTokenKind;
    start: number;
    end: number;
}

const KEYWORD_SET = new Set(RQL_KEYWORDS.map((entry) => entry.toLowerCase()));
const LITERAL_SET = new Set(RQL_LITERALS.map((entry) => entry.toLowerCase()));
const TYPE_SET = new Set(RQL_TYPES.map((entry) => entry.toLowerCase()));

const SYSTEM_COLUMN = /#(?:rownum|created_at|updated_at)\b/y;
const COMMENT = /#.*/y;
const NAMESPACE_HEAD = /[a-zA-Z_][\w$]*(?=\s*::)/y;
const NAMESPACE_SEPARATOR = /::/y;
const NAMED_ARGUMENT = /\w+\s*:(?!:)/y;
const VARIABLE = /\$[\w$]+/y;
const CALL_NAME = /[a-zA-Z_][\w$]*(?=\s*\()/y;
const WORD = /[a-zA-Z_][\w$]*/y;
const WHITESPACE = /[ \t\r\n]+/y;
const BRACKET = /[{}()[\]]/y;
const NUMBER = /[+-]?(?:[\d_]+(?:\.[\d_]+)?|\.[\d_]+)(?:[eE][+-]?\d+)?/y;
const OPERATOR = /<<|>>|\.\.|==|!=|->|=>|>=|<=|~=|:=|&&|\|\||\?\?|\/\/|[+\-*/%|.=<>!&^?]/y;
const DELIMITER = /[;,]/y;

function match(pattern: RegExp, source: string, position: number): string | null {
    pattern.lastIndex = position;
    const result = pattern.exec(source);
    return result === null ? null : result[0];
}

function classifyWord(word: string): RqlTokenKind {
    const lowered = word.toLowerCase();
    if (KEYWORD_SET.has(lowered)) return 'keyword';
    if (LITERAL_SET.has(lowered)) return 'constant';
    if (TYPE_SET.has(lowered)) return 'type';
    return 'identifier';
}

function classifyCall(word: string): RqlTokenKind {
    const lowered = word.toLowerCase();
    if (KEYWORD_SET.has(lowered)) return 'keyword';
    if (TYPE_SET.has(lowered)) return 'type';
    return 'function';
}

function scanString(source: string, position: number): number {
    const quote = source[position];
    let index = position + 1;
    while (index < source.length) {
        const char = source[index];
        if (char === '\\') {
            index += 2;
            continue;
        }
        index++;
        if (char === quote) break;
    }
    return Math.min(index, source.length);
}

export function tokenizeRql(source: string): RqlToken[] {
    const tokens: RqlToken[] = [];
    let position = 0;
    let afterSeparator = false;

    const push = (kind: RqlTokenKind, end: number): void => {
        tokens.push({kind, start: position, end});
        position = end;
    };

    while (position < source.length) {
        if (afterSeparator) {
            const white = match(WHITESPACE, source, position);
            if (white !== null) {
                push('white', position + white.length);
                continue;
            }

            const chained = match(NAMESPACE_HEAD, source, position);
            if (chained !== null) {
                afterSeparator = false;
                push('namespace', position + chained.length);
                continue;
            }

            const called = match(CALL_NAME, source, position);
            if (called !== null) {
                afterSeparator = false;
                push('function', position + called.length);
                continue;
            }

            const leaf = match(WORD, source, position);
            if (leaf !== null) {
                afterSeparator = false;
                push('entity', position + leaf.length);
                continue;
            }

            afterSeparator = false;
        }

        const systemColumn = match(SYSTEM_COLUMN, source, position);
        if (systemColumn !== null) {
            push('variable.predefined', position + systemColumn.length);
            continue;
        }

        const comment = match(COMMENT, source, position);
        if (comment !== null) {
            push('comment', position + comment.length);
            continue;
        }

        const namespace = match(NAMESPACE_HEAD, source, position);
        if (namespace !== null) {
            push('namespace', position + namespace.length);
            continue;
        }

        const separator = match(NAMESPACE_SEPARATOR, source, position);
        if (separator !== null) {
            afterSeparator = true;
            push('operator', position + separator.length);
            continue;
        }

        const namedArgument = match(NAMED_ARGUMENT, source, position);
        if (namedArgument !== null) {
            push('key', position + namedArgument.length);
            continue;
        }

        const variable = match(VARIABLE, source, position);
        if (variable !== null) {
            push('variable', position + variable.length);
            continue;
        }

        const call = match(CALL_NAME, source, position);
        if (call !== null) {
            push(classifyCall(call), position + call.length);
            continue;
        }

        const word = match(WORD, source, position);
        if (word !== null) {
            push(classifyWord(word), position + word.length);
            continue;
        }

        const white = match(WHITESPACE, source, position);
        if (white !== null) {
            push('white', position + white.length);
            continue;
        }

        const bracket = match(BRACKET, source, position);
        if (bracket !== null) {
            push('bracket', position + bracket.length);
            continue;
        }

        const number = match(NUMBER, source, position);
        if (number !== null) {
            push('number', position + number.length);
            continue;
        }

        const char = source[position];
        if (char === '"' || char === "'") {
            push('string', scanString(source, position));
            continue;
        }

        const operator = match(OPERATOR, source, position);
        if (operator !== null) {
            push('operator', position + operator.length);
            continue;
        }

        const delimiter = match(DELIMITER, source, position);
        if (delimiter !== null) {
            push('delimiter', position + delimiter.length);
            continue;
        }

        push('identifier', position + 1);
    }

    return tokens;
}
