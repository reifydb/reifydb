// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterEach, afterAll, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
// @ts-ignore
import React from 'react';
import {useConnection, ConnectionProvider, clearConnection} from '../../../src';
import {waitForDatabaseHttp} from '../setup';

describe.sequential('useConnection Hook (JSON HTTP)', () => {
    beforeAll(async () => {
        await waitForDatabaseHttp();
    }, 30000);

    beforeEach(async () => {
        await clearConnection();
    });

    afterEach(async () => {
        await clearConnection();
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clearConnection();
    });

    it.sequential('should connect manually without provider', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.isConnecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connectionError).toBeNull();
    });

    it.sequential('should auto-connect with ConnectionProvider', async () => {
        const wrapper = ({children}: { children: React.ReactNode }) => (
            <ConnectionProvider config={{url: process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091', token: process.env.REIFYDB_TOKEN, format: 'json'}} children={children}/>
        );

        const {result} = renderHook(() => useConnection(), {wrapper});

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.isConnecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connectionError).toBeNull();
    });

    it.sequential('should handle manual disconnect', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.isConnected).toBe(true));

        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.isConnected).toBe(false);
        expect(result.current.client).toBeNull();
    });

    it.sequential('should handle reconnection', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.isConnected).toBe(true));

        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.isConnected).toBe(false);

        await act(async () => {
            await result.current.reconnect();
        });

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });

    it.sequential('should not reconnect if already connected', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => expect(result.current.isConnected).toBe(true));

        const initialClient = result.current.client;

        await act(async () => {
            await result.current.connect();
        });

        expect(result.current.client).toBe(initialClient);
        expect(result.current.isConnected).toBe(true);
    });

    it.sequential('should handle connection with custom URL', async () => {
        const {result} = renderHook(() => useConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'}));

        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
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
            expect(result1.current.isConnected).toBe(true);
            expect(result2.current.isConnected).toBe(true);
        });

        expect(result1.current.client).toBe(result2.current.client);
    });
});
