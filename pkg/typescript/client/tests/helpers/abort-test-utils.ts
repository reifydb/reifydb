// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {vi} from 'vitest';

export function mock_fetch_success(body: any, status = 200): typeof fetch {
    return vi.fn().mockResolvedValue({
        ok: status >= 200 && status < 300,
        status,
        json: () => Promise.resolve(body),
        text: () => Promise.resolve(JSON.stringify(body)),
    });
}

export function mock_fetch_that_respects_signal(): typeof fetch {
    return vi.fn().mockImplementation((_url: string, init?: RequestInit) => {
        return new Promise((resolve, reject) => {
            if (init?.signal?.aborted) {
                reject(Object.assign(new Error('The operation was aborted'), {name: 'AbortError'}));
                return;
            }
            init?.signal?.addEventListener('abort', () => {
                reject(Object.assign(new Error('The operation was aborted'), {name: 'AbortError'}));
            });
        });
    });
}

export const LOGIN_SUCCESS_RESPONSE = {
    status: 'authenticated',
    token: 'test-token-123',
    identity: 'test-user',
};

export const FRAMES_SUCCESS_RESPONSE = {
    frames: [{
        columns: [{name: 'result', type: 'Int4', payload: ['42']}]
    }]
};

export interface MockSocket {
    readyState: number;
    close: ReturnType<typeof vi.fn>;
    send: ReturnType<typeof vi.fn>;
    addEventListener: ReturnType<typeof vi.fn>;
    removeEventListener: ReturnType<typeof vi.fn>;
    onmessage: ((event: any) => void) | null;
    onerror: ((event: any) => void) | null;
    onclose: (() => void) | null;
    _emit: (event: string, ...args: any[]) => void;
    _listeners: Record<string, Function[]>;
}

export function create_mock_socket(): MockSocket {
    const listeners: Record<string, Function[]> = {};
    return {
        readyState: 0,
        close: vi.fn(),
        send: vi.fn(),
        addEventListener: vi.fn((event: string, handler: Function) => {
            (listeners[event] ??= []).push(handler);
        }),
        removeEventListener: vi.fn((event: string, handler: Function) => {
            listeners[event] = (listeners[event] || []).filter(h => h !== handler);
        }),
        onmessage: null,
        onerror: null,
        onclose: null,
        _emit: (event: string, ...args: any[]) => {
            for (const handler of (listeners[event] || [])) handler(...args);
        },
        _listeners: listeners,
    };
}

export function setup_window_web_socket(mock_socket: MockSocket) {
    vi.stubGlobal('WebSocket', vi.fn().mockReturnValue(mock_socket));
    vi.stubGlobal('window', {WebSocket: globalThis.WebSocket});
}

export function teardown_window_web_socket() {
    vi.unstubAllGlobals();
}
