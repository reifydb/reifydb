// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {HttpClient} from '../src/http';
import {JsonHttpClient} from '../src/json-http';
import {mockFetchSuccess} from './helpers/abort-test-utils';

const INVALID_PARAMS_BODY = {
    error: "parameter $1: invalid data: cannot parse '0x0103904e0000000002000123' as Digest(Float8, 0.01)",
    code: 'INVALID_PARAMS',
};

describe('HTTP error code', () => {
    let originalFetch: typeof fetch;

    beforeEach(() => {
        originalFetch = globalThis.fetch;
    });

    afterEach(() => {
        vi.stubGlobal('fetch', originalFetch);
        vi.restoreAllMocks();
    });

    it.each([
        {format: 'frames'},
        {format: 'rbcf'},
    ] as const)('HttpClient [$format] keeps the server code of an error without a diagnostic', async ({format}) => {
        // A caller branches on the code as it does over ws, so dropping it leaves only message text to match.
        vi.stubGlobal('fetch', mockFetchSuccess(INVALID_PARAMS_BODY, 400));
        const client = HttpClient.connect({url: 'http://test', timeoutMs: 5000, token: 't', format});

        await expect(client.query('map { p: $1 }', [1], [])).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'INVALID_PARAMS',
            message: expect.stringContaining(INVALID_PARAMS_BODY.error),
        });
    });

    it('JsonHttpClient keeps the server code of an error without a diagnostic', async () => {
        // The json transport reads the same error body, so it must not drop the code either.
        vi.stubGlobal('fetch', mockFetchSuccess(INVALID_PARAMS_BODY, 400));
        const client = JsonHttpClient.connect({url: 'http://test', timeoutMs: 5000, token: 't'});

        await expect(client.query('map { p: $1 }', [1])).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'INVALID_PARAMS',
            message: expect.stringContaining(INVALID_PARAMS_BODY.error),
        });
    });
});
