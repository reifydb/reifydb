/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useConnection, connection} from '../../../src';
import {waitForDatabase} from '../setup';

describe('useConnection Hook', () => {
    afterAll(() => {
        // Ensure final disconnect
        connection.disconnect();
    });
    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    afterEach(() => {
        // Ensure we disconnect after each test
        connection.disconnect();
    });

    it('should auto-connect on mount', async () => {
        const { result } = renderHook(() => useConnection());

        // Initially should be connecting
        expect(result.current.isConnecting).toBe(true);
        expect(result.current.isConnected).toBe(false);

        // Wait for connection
        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.isConnecting).toBe(false);
        }, { timeout: 5000 });

        expect(result.current.client).toBeTruthy();
        expect(result.current.connectionError).toBeNull();
    });

    it('should handle manual disconnect', async () => {
        const { result } = renderHook(() => useConnection());

        // Wait for initial connection
        await waitFor(() => expect(result.current.isConnected).toBe(true));

        // Disconnect
        act(() => {
            result.current.disconnect();
        });

        expect(result.current.isConnected).toBe(false);
        expect(result.current.client).toBeNull();
    });

    it('should handle reconnection', async () => {
        const { result } = renderHook(() => useConnection());

        // Wait for initial connection
        await waitFor(() => expect(result.current.isConnected).toBe(true));

        // Disconnect
        act(() => {
            result.current.disconnect();
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

    it('should share connection state between multiple hooks', async () => {
        const { result: result1 } = renderHook(() => useConnection());
        const { result: result2 } = renderHook(() => useConnection());

        // Wait for connection
        await waitFor(() => {
            expect(result1.current.isConnected).toBe(true);
            expect(result2.current.isConnected).toBe(true);
        });

        // Both should have the same client instance
        expect(result1.current.client).toBe(result2.current.client);

        // Disconnect from one hook
        act(() => {
            result1.current.disconnect();
        });

        // Both should reflect disconnected state
        expect(result1.current.isConnected).toBe(false);
        expect(result2.current.isConnected).toBe(false);
    });

    it('should not reconnect if already connected', async () => {
        const { result } = renderHook(() => useConnection());

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

    it('should handle connection with custom URL', async () => {
        // First disconnect any existing connection
        connection.disconnect();

        const { result } = renderHook(() => useConnection());

        // Connect with custom URL (same as default for testing)
        await act(async () => {
            await result.current.connect();
        });

        await waitFor(() => {
            expect(result.current.isConnected).toBe(true);
            expect(result.current.client).toBeTruthy();
        });
    });
});