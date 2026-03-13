// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Lightweight Monarch tokenizer for testing — reimplements the core of Monaco's
// MonarchTokenizer._myTokenize() without browser/service dependencies.

// @ts-expect-error Monaco internal module — no type declarations
import { findRules, isFuzzyAction, isIAction, isString, sanitize, fixCase, substituteMatches } from 'monaco-editor/esm/vs/editor/standalone/common/monarch/monarchCommon.js';

export interface Token {
    offset: number;
    type: string;
}

interface StackElement {
    parent: StackElement | null;
    state: string;
    depth: number;
}

interface TokenizeLineResult {
    tokens: Token[];
    endState: StackElement;
}

function createStack(parent: StackElement | null, state: string): StackElement {
    return { parent, state, depth: (parent ? parent.depth : 0) + 1 };
}

function findBracket(lexer: any, matched: string): { token: string } | null {
    if (!matched) return null;
    matched = fixCase(lexer, matched);
    for (const bracket of lexer.brackets) {
        if (bracket.open === matched || bracket.close === matched) {
            return { token: bracket.token };
        }
    }
    return null;
}

function tokenizeLineInternal(lexer: any, line: string, initialStack?: StackElement): TokenizeLineResult {
    let stack: StackElement = initialStack || createStack(null, lexer.start || 'root');
    const tokens: Token[] = [];
    let pos = 0;
    let groupMatching: { matches: RegExpMatchArray; groups: { action: any; matched: string }[] } | null = null;
    let forceEvaluation = true;

    while (forceEvaluation || pos < line.length) {
        const pos0 = pos;
        const stackLen0 = stack.depth;
        const state = stack.state;
        const groupLen0 = groupMatching ? groupMatching.groups.length : 0;

        let matches: RegExpMatchArray | null = null;
        let matched: string | null = null;
        let action: any = null;

        if (groupMatching) {
            matches = groupMatching.matches;
            const groupEntry = groupMatching.groups.shift()!;
            matched = groupEntry.matched;
            action = groupEntry.action;
            if (groupMatching.groups.length === 0) {
                groupMatching = null;
            }
        } else {
            if (!forceEvaluation && pos >= line.length) break;
            forceEvaluation = false;

            let rules = lexer.tokenizer[state];
            if (!rules) {
                rules = findRules(lexer, state);
                if (!rules) throw new Error(`tokenizer state is not defined: ${state}`);
            }

            const restOfLine = line.substr(pos);
            for (const rule of rules) {
                if (pos === 0 || !rule.matchOnlyAtLineStart) {
                    matches = restOfLine.match(rule.resolveRegex(state));
                    if (matches) {
                        matched = matches[0];
                        action = rule.action;
                        break;
                    }
                }
            }
        }

        if (!matches) {
            matches = [''] as unknown as RegExpMatchArray;
            matched = '';
        }

        if (!action) {
            if (pos < line.length) {
                matches = [line.charAt(pos)] as unknown as RegExpMatchArray;
                matched = matches[0];
            }
            action = lexer.defaultToken;
        }

        if (matched === null) break;

        pos += matched.length;

        // Resolve fuzzy actions (cases dispatch)
        while (isFuzzyAction(action) && isIAction(action) && action.test) {
            action = action.test(matched, matches, state, pos === line.length);
        }

        let result: any = null;

        if (typeof action === 'string' || Array.isArray(action)) {
            result = action;
        } else if (action.group) {
            result = action.group;
        } else if (action.token !== null && action.token !== undefined) {
            if (action.tokenSubst) {
                result = substituteMatches(lexer, action.token, matched, matches, state);
            } else {
                result = action.token;
            }

            if (action.goBack) {
                pos = Math.max(0, pos - action.goBack);
            }

            if (action.next) {
                if (action.next === '@push') {
                    stack = createStack(stack, state);
                } else if (action.next === '@pop') {
                    if (stack.depth > 1) {
                        stack = stack.parent!;
                    }
                } else if (action.next === '@popall') {
                    while (stack.parent) stack = stack.parent;
                } else {
                    let nextState = substituteMatches(lexer, action.next, matched, matches, state);
                    if (nextState[0] === '@') nextState = nextState.substr(1);
                    stack = createStack(stack, nextState);
                }
            }
        }

        if (result === null) {
            throw new Error('lexer rule has no well-defined action');
        }

        // Handle group matches (array of sub-actions for capture groups)
        if (Array.isArray(result)) {
            if (matches!.length !== result.length + 1) {
                throw new Error('matched number of groups does not match the number of actions');
            }
            groupMatching = {
                matches: matches!,
                groups: result.map((act: any, i: number) => ({
                    action: act,
                    matched: matches![i + 1],
                })),
            };
            pos -= matched.length;
            continue;
        }

        // Handle @rematch
        if (result === '@rematch') {
            pos -= matched.length;
            matched = '';
            matches = null;
            result = '';
        }

        // Check progress
        if (matched.length === 0) {
            if (line.length === 0 || stackLen0 !== stack.depth || state !== stack.state ||
                (groupMatching ? groupMatching.groups.length : 0) !== groupLen0) {
                continue;
            } else {
                throw new Error('no progress in tokenizer');
            }
        }

        // Resolve token type — handle @brackets specially
        let tokenType: string;
        if (isString(result) && result.indexOf('@brackets') === 0) {
            const rest = result.substr('@brackets'.length);
            const bracket = findBracket(lexer, matched);
            if (!bracket) throw new Error(`@brackets token returned but no bracket defined as: ${matched}`);
            tokenType = sanitize(bracket.token + rest);
        } else {
            const token = result === '' ? '' : result + lexer.tokenPostfix;
            tokenType = sanitize(token);
        }

        tokens.push({ offset: pos0, type: tokenType });
    }

    return { tokens, endState: stack };
}

/**
 * Tokenize a single line and return the token array.
 */
export function tokenizeLine(lexer: any, line: string, initialStack?: StackElement): Token[] {
    return tokenizeLineInternal(lexer, line, initialStack).tokens;
}

/**
 * Tokenize multi-line text, passing state across lines.
 */
export function tokenize(lexer: any, text: string): (Token & { line: number })[] {
    const lines = text.split('\n');
    const allTokens: (Token & { line: number })[] = [];
    let stack: StackElement | undefined;

    for (let i = 0; i < lines.length; i++) {
        const result = tokenizeLineInternal(lexer, lines[i], stack);
        for (const token of result.tokens) {
            allTokens.push({ ...token, line: i });
        }
        stack = result.endState;
    }

    return allTokens;
}
