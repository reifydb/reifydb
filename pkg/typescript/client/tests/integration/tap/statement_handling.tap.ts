// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/**
 * Generated from: statement_handling.tap.md
 * TAP Test Specification for Statement Handling
 */

import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Client, WsClient } from '../../../src';
import { waitForDatabase } from '../setup';
import { Shape } from '@reifydb/core';

describe('Statement Handling', () => {
    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        try {
            wsClient = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            try {
                wsClient.disconnect();
            } catch (error) {
                console.error('⚠️ Error during disconnect:', error);
            }
            wsClient = null;
        }
    });

    describe('command', () => {
        
        it('no_statements', async () => {
            const frames = await wsClient.command(
                '',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single_empty_statement', async () => {
            const frames = await wsClient.command(
                ';',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('many_empty_statements', async () => {
            const frames = await wsClient.command(
                ';;;;;',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed_empty_and_non_empty', async () => {
            const frames = await wsClient.command(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}',
                null,
                [
                    Shape.object({ one: Shape.int4() }),
                    Shape.object({ two: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(2);

            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);
        }, 1000);

        it('single_statement_with_semicolon', async () => {
            const frames = await wsClient.command(
                'MAP {result: 1};',
                null,
                [Shape.object({ result: Shape.int4() })]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple_statements_same_structure', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP {result: 1};OUTPUT MAP {result: 2};MAP {result: 3};',
                null,
                [
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ result: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(3);

            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);

            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].result).toBe(2);

            // Frame 3
            expect(frames[2]).toHaveLength(1);
            expect(frames[2][0].result).toBe(3);
        }, 1000);

        it('multiple_statements_different_structure', async () => {
            const frames = await wsClient.command(
                "OUTPUT MAP {result: 1};OUTPUT MAP { a: 2, b: 3 };MAP {result: 'ReifyDB'};",
                null,
                [
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ a: Shape.int4(), b: Shape.int4() }),
                    Shape.object({ result: Shape.utf8() })
                ]
            );
            expect(frames).toHaveLength(3);
            
            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
            
            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);
            
            // Frame 3
            expect(frames[2]).toHaveLength(1);
            expect(frames[2][0].result).toBe('ReifyDB');
        }, 1000);

        it('statement_without_trailing_semicolon', async () => {
            const frames = await wsClient.command(
                'MAP {x: 1}',
                null,
                [Shape.object({ x: Shape.int4() })]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].x).toBe(1);
        }, 1000);

        it('multiple_statements_no_trailing_semicolon', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP {x: 1};MAP {y: 2}',
                null,
                [
                    Shape.object({ x: Shape.int4() }),
                    Shape.object({ y: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(2);

            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].x).toBe(1);

            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].y).toBe(2);
        }, 1000);

        it('statement_with_whitespace', async () => {
            const frames = await wsClient.command(
                '  OUTPUT MAP {result: 1}  ;  MAP {result: 2}  ',
                null,
                [
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ result: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(2);
            
            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
            
            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].result).toBe(2);
        }, 1000);
    });

    describe('query', () => {
        
        it('query_no_statements', async () => {
            const frames = await wsClient.query(
                '',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('query_single_empty_statement', async () => {
            const frames = await wsClient.query(
                ';',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('query_many_empty_statements', async () => {
            const frames = await wsClient.query(
                ';;;;;',
                null,
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('query_mixed_empty_and_non_empty', async () => {
            const frames = await wsClient.query(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}',
                null,
                [
                    Shape.object({ one: Shape.int4() }),
                    Shape.object({ two: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(2);

            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);
        }, 1000);

        it('query_single_statement_with_semicolon', async () => {
            const frames = await wsClient.query(
                'MAP {result: 1};',
                null,
                [Shape.object({ result: Shape.int4() })]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('query_multiple_statements_same_structure', async () => {
            const frames = await wsClient.query(
                'OUTPUT MAP {result: 1};OUTPUT MAP {result: 2};MAP {result: 3};',
                null,
                [
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ result: Shape.int4() })
                ]
            );
            expect(frames).toHaveLength(3);

            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);

            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].result).toBe(2);

            // Frame 3
            expect(frames[2]).toHaveLength(1);
            expect(frames[2][0].result).toBe(3);
        }, 1000);

        it('query_multiple_statements_different_structure', async () => {
            const frames = await wsClient.query(
                "OUTPUT MAP {result: 1};OUTPUT MAP { a: 2, b: 3 };MAP {result: 'ReifyDB'};",
                null,
                [
                    Shape.object({ result: Shape.int4() }),
                    Shape.object({ a: Shape.int4(), b: Shape.int4() }),
                    Shape.object({ result: Shape.utf8() })
                ]
            );
            expect(frames).toHaveLength(3);
            
            // Frame 1
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
            
            // Frame 2
            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);
            
            // Frame 3
            expect(frames[2]).toHaveLength(1);
            expect(frames[2][0].result).toBe('ReifyDB');
        }, 1000);
    });
});