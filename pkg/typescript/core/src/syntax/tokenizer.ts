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

function match(pattern: RegExp, source: string, charIndex: number): string | null {
    pattern.lastIndex = charIndex;
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

function scanString(source: string, charIndex: number): number {
    const quote = source[charIndex];
    let index = charIndex + 1;
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
    let charIndex = 0;
    let afterSeparator = false;

    const push = (kind: RqlTokenKind, end: number): void => {
        tokens.push({kind, start: charIndex, end});
        charIndex = end;
    };

    while (charIndex < source.length) {
        if (afterSeparator) {
            const white = match(WHITESPACE, source, charIndex);
            if (white !== null) {
                push('white', charIndex + white.length);
                continue;
            }

            const chained = match(NAMESPACE_HEAD, source, charIndex);
            if (chained !== null) {
                afterSeparator = false;
                push('namespace', charIndex + chained.length);
                continue;
            }

            const called = match(CALL_NAME, source, charIndex);
            if (called !== null) {
                afterSeparator = false;
                push('function', charIndex + called.length);
                continue;
            }

            const leaf = match(WORD, source, charIndex);
            if (leaf !== null) {
                afterSeparator = false;
                push('entity', charIndex + leaf.length);
                continue;
            }

            afterSeparator = false;
        }

        const systemColumn = match(SYSTEM_COLUMN, source, charIndex);
        if (systemColumn !== null) {
            push('variable.predefined', charIndex + systemColumn.length);
            continue;
        }

        const comment = match(COMMENT, source, charIndex);
        if (comment !== null) {
            push('comment', charIndex + comment.length);
            continue;
        }

        const namespace = match(NAMESPACE_HEAD, source, charIndex);
        if (namespace !== null) {
            push('namespace', charIndex + namespace.length);
            continue;
        }

        const separator = match(NAMESPACE_SEPARATOR, source, charIndex);
        if (separator !== null) {
            afterSeparator = true;
            push('operator', charIndex + separator.length);
            continue;
        }

        const namedArgument = match(NAMED_ARGUMENT, source, charIndex);
        if (namedArgument !== null) {
            push('key', charIndex + namedArgument.length);
            continue;
        }

        const variable = match(VARIABLE, source, charIndex);
        if (variable !== null) {
            push('variable', charIndex + variable.length);
            continue;
        }

        const call = match(CALL_NAME, source, charIndex);
        if (call !== null) {
            push(classifyCall(call), charIndex + call.length);
            continue;
        }

        const word = match(WORD, source, charIndex);
        if (word !== null) {
            push(classifyWord(word), charIndex + word.length);
            continue;
        }

        const white = match(WHITESPACE, source, charIndex);
        if (white !== null) {
            push('white', charIndex + white.length);
            continue;
        }

        const bracket = match(BRACKET, source, charIndex);
        if (bracket !== null) {
            push('bracket', charIndex + bracket.length);
            continue;
        }

        const number = match(NUMBER, source, charIndex);
        if (number !== null) {
            push('number', charIndex + number.length);
            continue;
        }

        const char = source[charIndex];
        if (char === '"' || char === "'") {
            push('string', scanString(source, charIndex));
            continue;
        }

        const operator = match(OPERATOR, source, charIndex);
        if (operator !== null) {
            push('operator', charIndex + operator.length);
            continue;
        }

        const delimiter = match(DELIMITER, source, charIndex);
        if (delimiter !== null) {
            push('delimiter', charIndex + delimiter.length);
            continue;
        }

        push('identifier', charIndex + 1);
    }

    return tokens;
}
