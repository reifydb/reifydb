// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from 'vitest';
// @ts-expect-error Monaco internal module — no type declarations
import { compile } from 'monaco-editor/esm/vs/editor/standalone/common/monarch/monarchCompile.js';
import { rqlLanguageDefinition } from '../../src/monaco/rql-language';
import { tokenize, tokenizeLine } from './monarch-tokenizer';

const lexer = compile('rql', rqlLanguageDefinition);

describe('RQL Monarch tokenization', () => {

    describe('compilation', () => {
        it('should compile without errors', () => {
            expect(lexer.languageId).toBe('rql');
            expect(lexer.defaultToken).toBe('');
            expect(lexer.ignoreCase).toBe(true);
        });
    });

    describe('keywords', () => {
        it.each([
            'FROM', 'FILTER', 'CREATE', 'NAMESPACE', 'TABLE',
            'INSERT', 'ASSERT', 'PROCEDURE', 'TEST', 'TESTS',
            'RUN', 'CALL', 'AS',
        ])('%s is tokenized as keyword', (kw) => {
            const tokens = tokenizeLine(lexer, kw);
            expect(tokens[0].type).toBe('keyword.rql');
        });

        it.each(['and', 'or', 'not', 'xor'])
        ('word operator "%s" is tokenized as keyword', (kw) => {
            const tokens = tokenizeLine(lexer, kw);
            expect(tokens[0].type).toBe('keyword.rql');
        });

        it('identifiers are not keywords', () => {
            const tokens = tokenizeLine(lexer, 'username');
            expect(tokens[0].type).toBe('identifier.rql');
        });
    });

    describe('namespace separator', () => {
        it('tp::users tokenizes as identifier :: identifier', () => {
            const tokens = tokenizeLine(lexer, 'tp::users');
            expect(tokens).toEqual([
                { offset: 0, type: 'identifier.rql' },
                { offset: 2, type: 'operator.rql' },
                { offset: 4, type: 'identifier.rql' },
            ]);
        });
    });

    describe('operators', () => {
        it.each([
            ['|', 'operator.rql'],
            ['==', 'operator.rql'],
            [':=', 'operator.rql'],
            ['<<', 'operator.rql'],
            ['..', 'operator.rql'],
        ])('"%s" tokenizes as %s', (op, expected) => {
            const tokens = tokenizeLine(lexer, `x ${op} y`);
            const opToken = tokens.find(t => t.offset > 0 && t.type.startsWith('operator'));
            expect(opToken).toBeDefined();
            expect(opToken!.type).toBe(expected);
        });
    });

    describe('delimiters', () => {
        it('; tokenizes as delimiter', () => {
            const tokens = tokenizeLine(lexer, 'x;');
            expect(tokens.at(-1)!.type).toBe('delimiter.rql');
        });

        it(', tokenizes as delimiter', () => {
            const tokens = tokenizeLine(lexer, 'a, b');
            const comma = tokens.find(t => t.type === 'delimiter.rql');
            expect(comma).toBeDefined();
        });
    });

    describe('named arguments vs namespace', () => {
        it('name: in struct literal tokenizes as key', () => {
            const tokens = tokenizeLine(lexer, 'name: value');
            expect(tokens[0].type).toBe('key.rql');
        });

        it('tp:: does NOT tokenize name: as key', () => {
            const tokens = tokenizeLine(lexer, 'tp::users');
            const keyTokens = tokens.filter(t => t.type === 'key.rql');
            expect(keyTokens).toHaveLength(0);
        });
    });

    describe('strings', () => {
        it('single-quoted string', () => {
            const tokens = tokenizeLine(lexer, "'hello'");
            const stringTokens = tokens.filter(t => t.type.startsWith('string'));
            expect(stringTokens.length).toBeGreaterThan(0);
        });

        it('double-quoted string', () => {
            const tokens = tokenizeLine(lexer, '"hello"');
            const stringTokens = tokens.filter(t => t.type.startsWith('string'));
            expect(stringTokens.length).toBeGreaterThan(0);
        });
    });

    describe('comments', () => {
        it('# line comment', () => {
            const tokens = tokenizeLine(lexer, '# this is a comment');
            expect(tokens[0].type).toBe('comment.rql');
        });
    });

    describe('full RQL statements', () => {
        it('FROM tp::users | FILTER name == \'Alice\'', () => {
            const tokens = tokenizeLine(lexer, "FROM tp::users | FILTER name == 'Alice'");
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            expect(types).toContain('operator.rql');
            expect(types).toContain('identifier.rql');
        });
    });

    // === New test cases ===

    describe('numbers', () => {
        it('integer 42', () => {
            const tokens = tokenizeLine(lexer, '42');
            expect(tokens[0].type).toBe('number.rql');
        });

        it('float 1.5', () => {
            const tokens = tokenizeLine(lexer, '1.5');
            expect(tokens[0].type).toBe('number.rql');
        });

        it('scientific notation 3.4028235e+38', () => {
            const tokens = tokenizeLine(lexer, '3.4028235e+38');
            expect(tokens).toHaveLength(1);
            expect(tokens[0].type).toBe('number.rql');
        });

        it('scientific notation negative exponent 1.175494e-38', () => {
            const tokens = tokenizeLine(lexer, '1.175494e-38');
            expect(tokens).toHaveLength(1);
            expect(tokens[0].type).toBe('number.rql');
        });

        it('zero', () => {
            const tokens = tokenizeLine(lexer, '0');
            expect(tokens[0].type).toBe('number.rql');
        });
    });

    describe('brackets', () => {
        it('{ is delimiter.curly', () => {
            const tokens = tokenizeLine(lexer, '{');
            expect(tokens[0].type).toBe('delimiter.curly.rql');
        });

        it('} is delimiter.curly', () => {
            const tokens = tokenizeLine(lexer, '}');
            expect(tokens[0].type).toBe('delimiter.curly.rql');
        });

        it('( is delimiter.parenthesis', () => {
            const tokens = tokenizeLine(lexer, '(');
            expect(tokens[0].type).toBe('delimiter.parenthesis.rql');
        });

        it(') is delimiter.parenthesis', () => {
            const tokens = tokenizeLine(lexer, ')');
            expect(tokens[0].type).toBe('delimiter.parenthesis.rql');
        });

        it('[ is delimiter.square', () => {
            const tokens = tokenizeLine(lexer, '[');
            expect(tokens[0].type).toBe('delimiter.square.rql');
        });

        it('] is delimiter.square', () => {
            const tokens = tokenizeLine(lexer, ']');
            expect(tokens[0].type).toBe('delimiter.square.rql');
        });
    });

    describe('constants', () => {
        it('true is constant', () => {
            const tokens = tokenizeLine(lexer, 'true');
            expect(tokens[0].type).toBe('constant.rql');
        });

        it('false is constant', () => {
            const tokens = tokenizeLine(lexer, 'false');
            expect(tokens[0].type).toBe('constant.rql');
        });

        it('none is constant', () => {
            const tokens = tokenizeLine(lexer, 'none');
            expect(tokens[0].type).toBe('constant.rql');
        });
    });

    describe('variables', () => {
        it('$x is variable', () => {
            const tokens = tokenizeLine(lexer, '$x');
            expect(tokens[0].type).toBe('variable.rql');
        });

        it('$variable_name is variable', () => {
            const tokens = tokenizeLine(lexer, '$variable_name');
            expect(tokens[0].type).toBe('variable.rql');
        });
    });

    describe('type keywords', () => {
        it('int4 is type', () => {
            const tokens = tokenizeLine(lexer, 'int4');
            expect(tokens[0].type).toBe('type.rql');
        });

        it('float8 is type', () => {
            const tokens = tokenizeLine(lexer, 'float8');
            expect(tokens[0].type).toBe('type.rql');
        });

        it('utf8 is type', () => {
            const tokens = tokenizeLine(lexer, 'utf8');
            expect(tokens[0].type).toBe('type.rql');
        });

        it('bool is type', () => {
            const tokens = tokenizeLine(lexer, 'bool');
            expect(tokens[0].type).toBe('type.rql');
        });

        it('Option is type', () => {
            const tokens = tokenizeLine(lexer, 'Option');
            expect(tokens[0].type).toBe('type.rql');
        });

        it.each([
            'int', 'uint', 'uuid4', 'blob', 'decimal',
            'datetime', 'time', 'duration', 'interval',
            'identityid', 'identity_id', 'dictionaryid', 'dictionary_id',
            'any', 'boolean', 'List', 'Record', 'Tuple',
        ])('%s is type', (type) => {
            const tokens = tokenizeLine(lexer, type);
            expect(tokens[0].type).toBe('type.rql');
        });
    });

    describe('block comments', () => {
        it('/* does not start a block comment', () => {
            const tokens = tokenizeLine(lexer, '/* comment */');
            expect(tokens.some(t => t.type === 'operator.rql')).toBe(true);
            expect(tokens.every(t => t.type === 'comment.rql')).toBe(false);
        });
    });

    describe('// as operator not comment', () => {
        it('a // b tokenizes // as operator', () => {
            const tokens = tokenizeLine(lexer, 'a // b');
            const slashSlash = tokens.find(t => t.offset === 2);
            expect(slashSlash).toBeDefined();
            expect(slashSlash!.type).toBe('operator.rql');
        });
    });

    describe('string escape sequences', () => {
        it('double-quoted string with escape', () => {
            const tokens = tokenizeLine(lexer, '"hello \\"world\\""');
            const escapeTokens = tokens.filter(t => t.type === 'string.escape.rql');
            expect(escapeTokens.length).toBeGreaterThan(0);
        });

        it('single-quoted string with escape', () => {
            const tokens = tokenizeLine(lexer, "'hello \\'world\\''");
            const escapeTokens = tokens.filter(t => t.type === 'string.escape.rql');
            expect(escapeTokens.length).toBeGreaterThan(0);
        });
    });

    describe('real-world RQL statements', () => {
        it('CREATE TABLE with types', () => {
            const tokens = tokenizeLine(lexer, 'CREATE TABLE test::data { id: int4, name: utf8, active: bool }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');       // CREATE, TABLE
            expect(types).toContain('type.rql');           // int4, utf8, bool
            expect(types).toContain('key.rql');            // id:, name:, active:
            expect(types).toContain('delimiter.curly.rql'); // { }
        });

        it('INSERT with namespace, brackets, number, string', () => {
            const tokens = tokenizeLine(lexer, "INSERT test::data [{ id: 1, name: 'Alice' }]");
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            expect(types).toContain('operator.rql');         // ::
            expect(types).toContain('number.rql');
            expect(types).toContain('delimiter.square.rql');
            expect(types).toContain('delimiter.curly.rql');
        });

        it('DISPATCH with triple :: namespace path', () => {
            const tokens = tokenizeLine(lexer, 'DISPATCH ns::order_event::OrderPlaced { id: 1 }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            const nsOps = tokens.filter(t => t.type === 'operator.rql');
            expect(nsOps.length).toBeGreaterThanOrEqual(2); // two :: separators
        });

        it('cast with scientific number and type', () => {
            const tokens = tokenizeLine(lexer, 'cast(3.4028235e+38, float4)');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');                  // cast
            expect(types).toContain('number.rql');                   // 3.4028235e+38
            expect(types).toContain('type.rql');                     // float4
            expect(types).toContain('delimiter.parenthesis.rql');    // ( )
        });

        it('let $x = 10; $x + 1', () => {
            const tokens = tokenizeLine(lexer, 'let $x = 10; $x + 1');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');    // let
            expect(types).toContain('variable.rql');   // $x
            expect(types).toContain('number.rql');     // 10, 1
            expect(types).toContain('operator.rql');   // =, +
        });

        it('sort with asc/desc', () => {
            const tokens = tokenizeLine(lexer, 'sort { col1:asc, col2:desc }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            expect(types).toContain('key.rql');
        });

        it('pipe chain: FROM | FILTER | ASSERT', () => {
            const tokens = tokenizeLine(lexer, 'FROM test::data | FILTER x > 0 | ASSERT { x == true }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            expect(types).toContain('operator.rql');
            expect(types).toContain('constant.rql');   // true
        });

        it('IF/ELSE with variable and brackets', () => {
            const tokens = tokenizeLine(lexer, 'IF $x > 3 { 1 } ELSE { 0 }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');    // IF, ELSE
            expect(types).toContain('variable.rql');   // $x
            expect(types).toContain('number.rql');
            expect(types).toContain('delimiter.curly.rql');
        });

        it('WHILE loop with variables', () => {
            const tokens = tokenizeLine(lexer, 'WHILE $i < 5 { $i = $i + 1 }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');
            expect(types).toContain('variable.rql');
            expect(types).toContain('number.rql');
        });

        it('namespace-qualified function call', () => {
            const tokens = tokenizeLine(lexer, "text::trim_start('  hello  ')");
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');                // text (module)
            expect(types).toContain('operator.rql');               // ::
            expect(types).toContain('identifier.rql');             // trim_start
        });

        it('Option(int4) type constructor', () => {
            const tokens = tokenizeLine(lexer, 'Option(int4)');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');                     // Option, int4
            expect(types).toContain('delimiter.parenthesis.rql');
        });

        it('CREATE TABLE with uuid7 and blob types', () => {
            const tokens = tokenizeLine(lexer, 'CREATE TABLE t { id: uuid7, data: blob }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');           // uuid7, blob
            expect(types).toContain('keyword.rql');        // CREATE, TABLE
        });

        it('CREATE TABLE with decimal and Option(utf8)', () => {
            const tokens = tokenizeLine(lexer, 'CREATE TABLE t { price: decimal, note: Option(utf8) }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');           // decimal, Option, utf8
        });

        it('CREATE TABLE with datetime and duration', () => {
            const tokens = tokenizeLine(lexer, 'CREATE TABLE t { created: datetime, elapsed: duration }');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');           // datetime, duration
        });

        it('cast with boolean alias', () => {
            const tokens = tokenizeLine(lexer, 'cast(x, boolean)');
            const types = tokens.map(t => t.type);
            expect(types).toContain('keyword.rql');        // cast
            expect(types).toContain('type.rql');           // boolean
        });

        it('Option(decimal) container type', () => {
            const tokens = tokenizeLine(lexer, 'Option(decimal)');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');           // Option, decimal
        });

        it('List(int4) container type', () => {
            const tokens = tokenizeLine(lexer, 'List(int4)');
            const types = tokens.map(t => t.type);
            expect(types).toContain('type.rql');           // List, int4
        });

        it('filter with constant', () => {
            const tokens = tokenizeLine(lexer, 'filter active == true');
            const types = tokens.map(t => t.type);
            expect(types[0]).toBe('keyword.rql');      // filter
            expect(types).toContain('identifier.rql'); // active
            expect(types).toContain('operator.rql');   // ==
            expect(types).toContain('constant.rql');   // true
        });
    });
});
