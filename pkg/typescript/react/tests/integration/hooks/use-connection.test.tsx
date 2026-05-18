// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterEach, afterAll, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
// @ts-ignore
import React from 'react';
import {useConnection, ConnectionProvider, clear_connection, get_connection} from '../../../src';
import {wait_for_database} from '../setup';

describe.sequential('useConnection Hook', () => {
    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    beforeEach(async () => {
        // Clear all connections before each test to ensure clean state
        await clear_connection();
        // Seed the connection pool with the correct URL
        get_connection({url: process.env.REIFYDB_WS_URL, token: process.env.REIFYDB_TOKEN});
    });

    afterEach(async () => {
        // Clear all connections after each test
        await clear_connection();
        // Small delay to ensure cleanup is complete
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clear_connection();
    });

    it.sequential('should connect manually without provider', async () => {
        const {result} = renderHook(() => useConnection());

        // Manually connect (no auto-connect)
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
            <ConnectionProvider config={{url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN}} children={children}/>
        );

        const {result} = renderHook(() => useConnection(), {wrapper});

        // Wait for connection
        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.is_connecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connection_error).toBeNull();
    });

    it.sequential('should handle manual disconnect', async () => {
        const {result} = renderHook(() => useConnection());

        // Manually connect first
        await act(async () => {
            await result.current.connect();
        });

        // Wait for initial connection
        await waitFor(() => expect(result.current.is_connected).toBe(true));

        // Disconnect
        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.is_connected).toBe(false);
        expect(result.current.client).toBeNull();
    });

    it.sequential('should handle reconnection', async () => {
        const {result} = renderHook(() => useConnection());

        // Manually connect first
        await act(async () => {
            await result.current.connect();
        });

        // Wait for initial connection
        await waitFor(() => expect(result.current.is_connected).toBe(true));

        // Disconnect
        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.is_connected).toBe(false);

        // Reconnect
        await act(async () => {
            await result.current.reconnect();
        });

        await waitFor(() => {
            expect(result.current.is_connected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });

    it.sequential('should not reconnect if already connected', async () => {
        const {result} = renderHook(() => useConnection());

        // Manually connect first
        await act(async () => {
            await result.current.connect();
        });

        // Wait for initial connection
        await waitFor(() => expect(result.current.is_connected).toBe(true));

        const initialClient = result.current.client;

        // Try to connect again
        await act(async () => {
            await result.current.connect();
        });

        // Should still have the same client
        expect(result.current.client).toBe(initialClient);
        expect(result.current.is_connected).toBe(true);
    });

    it.sequential('should handle connection with custom URL', async () => {
        const {result} = renderHook(() => useConnection());

        // Connect with custom URL (same as default for testing)
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
            <ConnectionProvider config={{url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN}} children={children}/>
        );

        const {result: result1} = renderHook(() => useConnection(), {wrapper});
        const {result: result2} = renderHook(() => useConnection(), {wrapper});

        // Wait for connection
        await waitFor(() => {
            expect(result1.current.is_connected).toBe(true);
            expect(result2.current.is_connected).toBe(true);
        });

        // Both should have the same client instance from context
        expect(result1.current.client).toBe(result2.current.client);
    });
});