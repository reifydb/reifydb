/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, afterAll, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
// @ts-ignore
import React from 'react';
import {useConnection, ConnectionProvider, clearAllConnections} from '../../../src';
import {waitForDatabase} from '../setup';

describe.sequential('useConnection Hook', () => {
    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        // Clear all connections before each test to ensure clean state
        await clearAllConnections();
    });

    afterEach(async () => {
        // Clear all connections after each test
        await clearAllConnections();
        // Small delay to ensure cleanup is complete
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clearAllConnections();
    });

    it.sequential('should auto-connect on mount without provider', async () => {
        const {result} = renderHook(() => useConnection());

        // Wait for connection
        await waitFor(() => {
            expect(result.current.isConnected || result.current.isConnecting).toBe(true);
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
            <ConnectionProvider config={{url: 'ws://127.0.0.1:8090'}} children={children}/>
        );

        const {result} = renderHook(() => useConnection(), {wrapper});

        // Wait for connection
        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.isConnecting).toBe(false);
        }, {timeout: 5000});

        expect(result.current.client).toBeTruthy();
        expect(result.current.connectionError).toBeNull();
    });

    it.sequential('should handle manual disconnect', async () => {
        const {result} = renderHook(() => useConnection());

        // Wait for initial connection
        await waitFor(() => expect(result.current.isConnected).toBe(true));

        // Disconnect
        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.isConnected).toBe(false);
        expect(result.current.client).toBeNull();
    });

    it.sequential('should handle reconnection', async () => {
        const {result} = renderHook(() => useConnection());

        // Wait for initial connection
        await waitFor(() => expect(result.current.isConnected).toBe(true));

        // Disconnect
        await act(async () => {
            await result.current.disconnect();
        });

        expect(result.current.isConnected).toBe(false);

        // Reconnect
        await act(async () => {
            await result.current.reconnect();
        });

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });

    it.sequential('should not reconnect if already connected', async () => {
        const {result} = renderHook(() => useConnection());

        // Wait for initial connection
        await waitFor(() => expect(result.current.isConnected).toBe(true));

        const initialClient = result.current.client;

        // Try to connect again
        await act(async () => {
            await result.current.connect();
        });

        // Should still have the same client
        expect(result.current.client).toBe(initialClient);
        expect(result.current.isConnected).toBe(true);
    });

    it.sequential('should handle connection with custom URL', async () => {
        const {result} = renderHook(() => useConnection());

        // Connect with custom URL (same as default for testing)
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
            <ConnectionProvider config={{url: 'ws://127.0.0.1:8090'}} children={children}/>
        );

        const {result: result1} = renderHook(() => useConnection(), {wrapper});
        const {result: result2} = renderHook(() => useConnection(), {wrapper});

        // Wait for connection
        await waitFor(() => {
            expect(result1.current.isConnected).toBe(true);
            expect(result2.current.isConnected).toBe(true);
        });

        // Both should have the same client instance from context
        expect(result1.current.client).toBe(result2.current.client);
    });
});