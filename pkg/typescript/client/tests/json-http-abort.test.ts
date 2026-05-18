// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {JsonHttpClient} from '../src/json-http';
import {
    mock_fetch_success,
    mock_fetch_that_respects_signal,
    LOGIN_SUCCESS_RESPONSE,
} from './helpers/abort-test-utils';

describe('JsonHttpClient abort signal', () => {
    let original_fetch: typeof fetch;

    beforeEach(() => {
        original_fetch = globalThis.fetch;
    });

    afterEach(() => {
        vi.stubGlobal('fetch', original_fetch);
        vi.restoreAllMocks();
        vi.useRealTimers();
    });

    function create_client(opts: { timeout_ms?: number; token?: string } = {}) {
        return JsonHttpClient.connect({
            url: 'http://test',
            timeout_ms: opts.timeout_ms ?? 5000,
            token: opts.token,
        });
    }

    async function login_client(client: JsonHttpClient) {
        vi.stubGlobal('fetch', mock_fetch_success(LOGIN_SUCCESS_RESPONSE));
        await client.login_with_password('user', 'pass');
    }

    describe('login', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_success(LOGIN_SUCCESS_RESPONSE));
            const controller = new AbortController();
            const client = create_client();

            const result = await client.login_with_password('user', 'pass', {signal: controller.signal});
            expect(result.token).toBe('test-token-123');
            expect(result.identity).toBe('test-user');
        });

        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            controller.abort();
            const client = create_client();

            await expect(client.login_with_password('user', 'pass', {signal: controller.signal}))
                .rejects.toThrow('Login timeout or aborted');
        });

        it('rejects when signal is aborted during request', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            const client = create_client();

            const promise = client.login_with_password('user', 'pass', {signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('Login timeout or aborted');
        });
    });

    describe('logout', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            const client = create_client();
            await login_client(client);

            vi.stubGlobal('fetch', mock_fetch_success({}, 200));
            const controller = new AbortController();

            await client.logout({signal: controller.signal});
        });

        it('rejects when signal is pre-aborted', async () => {
            const client = create_client();
            await login_client(client);

            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            controller.abort();

            await expect(client.logout({signal: controller.signal}))
                .rejects.toThrow('Logout timeout or aborted');
        });

        it('rejects when signal is aborted during request', async () => {
            const client = create_client();
            await login_client(client);

            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();

            const promise = client.logout({signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('Logout timeout or aborted');
        });
    });

    describe('query (send)', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_success({data: 'ok'}));
            const controller = new AbortController();
            const client = create_client({token: 'tok'});

            const result = await client.query('MAP {result: 42}', {}, {signal: controller.signal});
            expect(result).toBeDefined();
        });

        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            controller.abort();
            const client = create_client({token: 'tok'});

            await expect(client.query('MAP 1', {}, {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });

        it('rejects when signal is aborted during request', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            const client = create_client({token: 'tok'});

            const promise = client.query('MAP 1', {}, {signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('admin (send)', () => {
        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            controller.abort();
            const client = create_client({token: 'tok'});

            await expect(client.admin('MAP 1', {}, {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('command (send)', () => {
        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            controller.abort();
            const client = create_client({token: 'tok'});

            await expect(client.command('MAP 1', {}, {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('AbortSignal.any fallback', () => {
        it('aborts via addEventListener fallback when AbortSignal.any is unavailable', async () => {
            const original_any = (AbortSignal as any).any;
            delete (AbortSignal as any).any;

            try {
                vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
                const controller = new AbortController();
                const client = create_client();

                const promise = client.login_with_password('user', 'pass', {signal: controller.signal});
                controller.abort();

                await expect(promise).rejects.toThrow('Login timeout or aborted');
            } finally {
                (AbortSignal as any).any = original_any;
            }
        });
    });

    describe('timeout with signal', () => {
        it('times out even when a user signal is provided and not aborted', async () => {
            vi.useFakeTimers();
            vi.stubGlobal('fetch', mock_fetch_that_respects_signal());
            const controller = new AbortController();
            const client = create_client({token: 'tok', timeout_ms: 10});

            const promise = client.query('MAP 1', {}, {signal: controller.signal});
            vi.advanceTimersByTime(10);

            await expect(promise).rejects.toThrow('ReifyDB query timeout');
        });
    });
});
