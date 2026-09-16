// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {HttpClient} from '../src/http';
import {WsClient} from '../src/ws';
import {decodeJsonResponse} from '../src/json-decode';
import {
    BinaryKind,
    decodeBatchEnvelope,
    decodeEnvelope,
    dispatchChange,
    type SubscriptionTarget,
} from '../src/subscription-decode';
import {CONTENT_TYPE_FRAMES} from '../src/content-types';
import {
    createMockSocket,
    mockFetchSuccess,
    MockSocket,
    setupWindowWebSocket,
    teardownWindowWebSocket,
} from './helpers/abort-test-utils';

const INT4 = {id: 'Int4'};

function deliver(body: any) {
    const seen = {errors: [] as unknown[], inserts: [] as any[][], updates: [] as any[][], removes: [] as any[][]};
    const target: SubscriptionTarget = {
        callbacks: {
            onInsert: rows => seen.inserts.push(rows),
            onUpdate: rows => seen.updates.push(rows),
            onRemove: rows => seen.removes.push(rows),
            onError: error => seen.errors.push(error),
        },
    };
    try {
        dispatchChange(target, CONTENT_TYPE_FRAMES, body);
    } catch (error) {
        seen.errors.push(error);
    }
    return seen;
}

function u32(n: number): number[] {
    return [n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff];
}

function utf8(text: string): number[] {
    return Array.from(new TextEncoder().encode(text));
}

function envelopeBytes(kind: number, id: string, meta: string): Uint8Array {
    const idBytes = utf8(id);
    const metaBytes = utf8(meta);
    return new Uint8Array([kind, ...u32(idBytes.length), ...idBytes, ...u32(metaBytes.length), ...metaBytes]);
}

describe('dispatchChange rejects malformed change bodies', () => {
    it('reports a body without frames', () => {
        // a malformed body must not read as zero frames
        expect(deliver({}).errors.length).toBeGreaterThan(0);
    });

    it('reports frames that are not an array', () => {
        // a string of frames must not be iterated character by character into nothing
        expect(deliver({frames: 'abc'}).errors.length).toBeGreaterThan(0);
    });

    it('reports a frame without columns', () => {
        // a frame missing its columns must not deliver as an empty change
        expect(deliver({frames: [{op: 1}]}).errors.length).toBeGreaterThan(0);
    });

    it('does not deliver a frame with an unknown op as an insert', () => {
        // only ops 1, 2 and 3 exist, so an unknown op must never fall through to onInsert
        const seen = deliver({frames: [{op: 7, columns: [{name: 'a', type: INT4, payload: ['1']}]}]});
        expect(seen.inserts).toEqual([]);
        expect(seen.errors.length).toBeGreaterThan(0);
    });

    it('does not deliver a row number that is not a number', () => {
        // a garbage row number must not reach the caller as NaN
        const seen = deliver({frames: [{op: 1, row_numbers: ['abc'], columns: [{name: 'a', type: INT4, payload: ['1']}]}]});
        expect(seen.inserts).toEqual([]);
        expect(seen.errors.length).toBeGreaterThan(0);
    });

    it('does not deliver rows when row numbers are fewer than rows', () => {
        // row numbers must cover every row, otherwise some rows silently lose their identity
        const seen = deliver({frames: [{op: 1, row_numbers: [5], columns: [{name: 'a', type: INT4, payload: ['1', '2']}]}]});
        expect(seen.inserts).toEqual([]);
        expect(seen.errors.length).toBeGreaterThan(0);
    });
});

describe('binary envelopes reject malformed bytes', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('rejects metadata that is not JSON', () => {
        // unparsable metadata must fail loud instead of being logged and dropped
        vi.spyOn(console, 'error').mockImplementation(() => {});
        expect(() => decodeEnvelope(envelopeBytes(BinaryKind.Response, 'req-1', '{not json'))).toThrow();
    });

    it('rejects an envelope whose id runs past the end', () => {
        // a truncated envelope must not come back as null, which the socket handler drops silently
        expect(() => decodeEnvelope(new Uint8Array([BinaryKind.Response, ...u32(100), 0x61]))).toThrow();
    });

    it('rejects an envelope with an unknown kind', () => {
        // only Response, Change and BatchChange exist, so another kind must not be dropped silently
        expect(() => decodeEnvelope(envelopeBytes(9, 'req-1', ''))).toThrow();
    });

    it('rejects a batch envelope whose entry runs past the end', () => {
        // a truncated batch must not come back as null, which the socket handler drops silently
        const bytes = new Uint8Array([BinaryKind.BatchChange, ...u32(1), 0x62, ...u32(1), ...u32(50), 0x73]);
        expect(() => decodeBatchEnvelope(bytes)).toThrow();
    });

    it('rejects a batch envelope with bytes after its last entry', () => {
        // bytes past the declared entries mean the entry count was wrong
        const bytes = new Uint8Array([BinaryKind.BatchChange, ...u32(1), 0x62, ...u32(0), 0xde, 0xad]);
        expect(() => decodeBatchEnvelope(bytes)).toThrow();
    });
});

describe('decodeJsonResponse rejects malformed envelopes', () => {
    it('rejects an envelope with rows but no types', () => {
        // without types every column is lost, so the rows must not silently vanish
        expect(() => decodeJsonResponse([{rows: [{a: '1'}]}])).toThrow();
    });
});

describe('frames transports reject a body without frames', () => {
    let originalFetch: typeof fetch;
    let mockSocket: MockSocket;

    beforeEach(() => {
        originalFetch = globalThis.fetch;
        mockSocket = createMockSocket();
        mockSocket.readyState = 1;
        setupWindowWebSocket(mockSocket);
    });

    afterEach(() => {
        vi.stubGlobal('fetch', originalFetch);
        teardownWindowWebSocket();
        vi.restoreAllMocks();
    });

    it('HttpClient rejects a frames response without frames', async () => {
        // the server always writes a frames key, so its absence must not read as zero frames
        vi.stubGlobal('fetch', mockFetchSuccess({}));
        const client = HttpClient.connect({url: 'http://test', timeoutMs: 5000, format: 'frames'});

        await expect(client.query('from test::t', undefined, [])).rejects.toThrow();
    });

    it('WsClient rejects a frames response without frames', async () => {
        // the server always writes a frames key, so its absence must not read as zero frames
        const client = await WsClient.connect({url: 'ws://test', format: 'frames'});
        const pending = client.query('from test::t', undefined, []);
        const request = JSON.parse(mockSocket.send.mock.calls.at(-1)![0]);
        mockSocket.onmessage!({data: JSON.stringify({id: request.id, type: 'Query', payload: {content_type: CONTENT_TYPE_FRAMES, body: {}}})});

        await expect(pending).rejects.toThrow();
        client.disconnect();
    });
});

describe('frameToRows rejects ragged columns', () => {
    const UTF8 = {id: 'Utf8'};

    it('does not deliver a frame whose later column is shorter than the first', () => {
        // the row count comes from the first column alone, so a short later column decodes its missing cells as none
        const seen = deliver({
            frames: [{
                op: 1,
                columns: [
                    {name: 'a', type: INT4, payload: ['1', '2']},
                    {name: 'b', type: UTF8, payload: ['x']},
                ],
            }],
        });
        expect(seen.inserts).toEqual([]);
        expect(seen.errors.length).toBeGreaterThan(0);
    });

    it('does not deliver a frame whose first column is shorter than a later one', () => {
        // rows past the first column's length are dropped, so the change would arrive silently incomplete
        const seen = deliver({
            frames: [{
                op: 1,
                columns: [
                    {name: 'a', type: INT4, payload: ['1']},
                    {name: 'b', type: INT4, payload: ['1', '2']},
                ],
            }],
        });
        expect(seen.inserts).toEqual([]);
        expect(seen.errors.length).toBeGreaterThan(0);
    });
});
