// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterEach, afterAll, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
// @ts-ignore
import React from 'react';
import {useConnection, ConnectionProvider, clear_connection} from '../../../src';
import {wait_for_database_http} from '../setup';

describe.sequential('useConnection Hook (JSON HTTP)', () => {
    beforeAll(async () => {
        await wait_for_database_http();
    }, 30000);

    beforeEach(async () => {
        await clear_connection();
    });

    afterEach(async () => {
        await clear_connection();
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clear_connection();
    });

    it.sequential('should connect manually without provider', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.is_connecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connection_error).toBeNull();
    });

    it.sequential('should auto-connect with ConnectionProvider', async () => {
        const wrapper = ({children}: { children: React.ReactNode }) => (
            <ConnectionProvider config={{url: process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091', token: process.env.REIFYDB_TOKEN, format: 'json'}} children={children}/>
        );

        const {result} = renderHook(() => useConnection(), {wrapper});

        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.is_connecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connection_error).toBeNull();
    });

    it.sequential('should handle manual disconnect', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.is_connected).toBe(true));

        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.is_connected).toBe(false);
        expect(result.current.client).toBeNull();
    });

    it.sequential('should handle reconnection', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.is_connected).toBe(true));

        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.is_connected).toBe(false);

        await act(async () => {
            await result.current.reconnect();
        });

        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });

    it.sequential('should not reconnect if already connected', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.is_connected).toBe(true));

        const initialClient = result.current.client;

        await act(async () => {
            await result.current.connect();
        });

        expect(result.current.client).toBe(initialClient);
        expect(result.current.is_connected).toBe(true);
    });

    it.sequential('should handle connection with custom URL', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });

    it.sequential('should share state within ConnectionProvider', async () => {
        const wrapper = ({children}: { children: React.ReactNode }) => (
            <ConnectionProvider config={{url: process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091', token: process.env.REIFYDB_TOKEN, format: 'json'}} children={children}/>
        );

        const {result: result1} = renderHook(() => useConnection(), {wrapper});
        const {result: result2} = renderHook(() => useConnection(), {wrapper});

        await waitFor(() => {
            expect(result1.current.is_connected).toBe(true);
            expect(result2.current.is_connected).toBe(true);
        });

        expect(result1.current.client).toBe(result2.current.client);
    });
});
