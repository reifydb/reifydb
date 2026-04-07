// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {HttpClient} from '../src/http';
import {
    mockFetchSuccess,
    mockFetchThatRespectsSignal,
    LOGIN_SUCCESS_RESPONSE,
    FRAMES_SUCCESS_RESPONSE,
} from './helpers/abort-test-utils';

describe('HttpClient abort signal', () => {
    let originalFetch: typeof fetch;

    beforeEach(() => {
        originalFetch = globalThis.fetch;
    });

    afterEach(() => {
        vi.stubGlobal('fetch', originalFetch);
        vi.restoreAllMocks();
        vi.useRealTimers();
    });

    function createClient(opts: { timeoutMs?: number; token?: string } = {}) {
        return HttpClient.connect({
            url: 'http://test',
            timeoutMs: opts.timeoutMs ?? 5000,
            token: opts.token,
        });
    }

    async function loginClient(client: HttpClient) {
        vi.stubGlobal('fetch', mockFetchSuccess(LOGIN_SUCCESS_RESPONSE));
        await client.loginWithPassword('user', 'pass');
    }

    describe('login', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            vi.stubGlobal('fetch', mockFetchSuccess(LOGIN_SUCCESS_RESPONSE));
            const controller = new AbortController();
            const client = createClient();

            const result = await client.loginWithPassword('user', 'pass', {signal: controller.signal});
            expect(result.token).toBe('test-token-123');
            expect(result.identity).toBe('test-user');
        });

        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            controller.abort();
            const client = createClient();

            await expect(client.loginWithPassword('user', 'pass', {signal: controller.signal}))
                .rejects.toThrow('Login timeout or aborted');
        });

        it('rejects when signal is aborted during request', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            const client = createClient();

            const promise = client.loginWithPassword('user', 'pass', {signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('Login timeout or aborted');
        });
    });

    describe('logout', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            const client = createClient();
            await loginClient(client);

            vi.stubGlobal('fetch', mockFetchSuccess({}, 200));
            const controller = new AbortController();

            await client.logout({signal: controller.signal});
        });

        it('rejects when signal is pre-aborted', async () => {
            const client = createClient();
            await loginClient(client);

            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            controller.abort();

            await expect(client.logout({signal: controller.signal}))
                .rejects.toThrow('Logout timeout or aborted');
        });

        it('rejects when signal is aborted during request', async () => {
            const client = createClient();
            await loginClient(client);

            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();

            const promise = client.logout({signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('Logout timeout or aborted');
        });
    });

    describe('query (send)', () => {
        it('completes normally when signal is provided but not aborted', async () => {
            vi.stubGlobal('fetch', mockFetchSuccess(FRAMES_SUCCESS_RESPONSE));
            const controller = new AbortController();
            const client = createClient({token: 'tok'});

            const frames = await client.query('MAP {result: 42}', {}, [], {signal: controller.signal});
            expect(frames).toBeDefined();
        });

        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            controller.abort();
            const client = createClient({token: 'tok'});

            await expect(client.query('MAP 1', {}, [], {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });

        it('rejects when signal is aborted during request', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            const client = createClient({token: 'tok'});

            const promise = client.query('MAP 1', {}, [], {signal: controller.signal});
            controller.abort();

            await expect(promise).rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('admin (send)', () => {
        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            controller.abort();
            const client = createClient({token: 'tok'});

            await expect(client.admin('MAP 1', {}, [], {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('command (send)', () => {
        it('rejects when signal is pre-aborted', async () => {
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            controller.abort();
            const client = createClient({token: 'tok'});

            await expect(client.command('MAP 1', {}, [], {signal: controller.signal}))
                .rejects.toThrow('ReifyDB query timeout');
        });
    });

    describe('AbortSignal.any fallback', () => {
        it('aborts via addEventListener fallback when AbortSignal.any is unavailable', async () => {
            const originalAny = (AbortSignal as any).any;
            delete (AbortSignal as any).any;

            try {
                vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
                const controller = new AbortController();
                const client = createClient();

                const promise = client.loginWithPassword('user', 'pass', {signal: controller.signal});
                controller.abort();

                await expect(promise).rejects.toThrow('Login timeout or aborted');
            } finally {
                (AbortSignal as any).any = originalAny;
            }
        });
    });

    describe('timeout with signal', () => {
        it('times out even when a user signal is provided and not aborted', async () => {
            vi.useFakeTimers();
            vi.stubGlobal('fetch', mockFetchThatRespectsSignal());
            const controller = new AbortController();
            const client = createClient({token: 'tok', timeoutMs: 10});

            const promise = client.query('MAP 1', {}, [], {signal: controller.signal});
            vi.advanceTimersByTime(10);

            await expect(promise).rejects.toThrow('ReifyDB query timeout');
        });
    });
});
