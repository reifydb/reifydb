// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {tokenize_rql, type RqlToken} from '../../src/syntax';

function spans(source: string): Array<[string, string]> {
    return tokenize_rql(source)
        .filter((token: RqlToken) => token.kind !== 'white')
        .map((token: RqlToken) => [token.kind, source.slice(token.start, token.end)]);
}

describe('tokenize_rql', () => {
    it('covers the source exactly once, in order', () => {
        // a gap or overlap would drop or duplicate characters when the shell repaints the line
        const source = 'from app::users map { name, age } take 3;';
        let cursor = 0;
        for (const token of tokenize_rql(source)) {
            expect(token.start).toBe(cursor);
            expect(token.end).toBeGreaterThan(token.start);
            cursor = token.end;
        }
        expect(cursor).toBe(source.length);
    });

    it('splits a namespace path into namespace and entity', () => {
        // the leaf is the table, the head is the namespace - coloring them alike loses the distinction
        expect(spans('from app::users')).toEqual([
            ['keyword', 'from'],
            ['namespace', 'app'],
            ['operator', '::'],
            ['entity', 'users'],
        ]);
    });

    it('classifies a chained path head as namespace and the leaf as entity', () => {
        expect(spans('ns::a::b')).toEqual([
            ['namespace', 'ns'],
            ['operator', '::'],
            ['namespace', 'a'],
            ['operator', '::'],
            ['entity', 'b'],
        ]);
    });

    it('classifies a namespaced call as a function', () => {
        expect(spans('math::avg(x)')).toEqual([
            ['namespace', 'math'],
            ['operator', '::'],
            ['function', 'avg'],
            ['bracket', '('],
            ['identifier', 'x'],
            ['bracket', ')'],
        ]);
    });

    it('keeps a keyword-named path segment a namespace', () => {
        // `table` is a keyword, but in `table::x` it is a path head - keyword coloring there is wrong
        expect(spans('table::x')[0]).toEqual(['namespace', 'table']);
    });

    it('reads system columns before line comments', () => {
        // both start with `#`; comment-first would swallow the rest of the statement
        expect(spans('#rownum # trailing note')).toEqual([
            ['variable.predefined', '#rownum'],
            ['comment', '# trailing note'],
        ]);
    });

    it('matches keywords case-insensitively', () => {
        expect(spans('FROM x')[0]).toEqual(['keyword', 'FROM']);
    });

    it('separates named arguments from the namespace separator', () => {
        expect(spans('with { ttl: 5 }')).toEqual([
            ['keyword', 'with'],
            ['bracket', '{'],
            ['key', 'ttl:'],
            ['number', '5'],
            ['bracket', '}'],
        ]);
    });

    it('classifies literals as constants, not identifiers', () => {
        expect(spans('none true false')).toEqual([
            ['constant', 'none'],
            ['constant', 'true'],
            ['constant', 'false'],
        ]);
    });

    it('classifies cast targets as types', () => {
        expect(spans('cast(x, int4)')).toEqual([
            ['keyword', 'cast'],
            ['bracket', '('],
            ['identifier', 'x'],
            ['delimiter', ','],
            ['type', 'int4'],
            ['bracket', ')'],
        ]);
    });

    it('treats an unterminated string as a string to the end of input', () => {
        // the line is tokenized on every keystroke, so a half-typed literal must not derail the rest
        expect(spans('map { a: "hello')).toEqual([
            ['keyword', 'map'],
            ['bracket', '{'],
            ['key', 'a:'],
            ['string', '"hello'],
        ]);
    });

    it('does not end a string on an escaped quote', () => {
        expect(spans('"a\\"b" x')).toEqual([
            ['string', '"a\\"b"'],
            ['identifier', 'x'],
        ]);
    });

    it('reads numbers with underscores and exponents as one token', () => {
        expect(spans('take 1_000e3')).toEqual([
            ['keyword', 'take'],
            ['number', '1_000e3'],
        ]);
    });

    it('reads variables as one token', () => {
        expect(spans('$limit')).toEqual([['variable', '$limit']]);
    });
});
