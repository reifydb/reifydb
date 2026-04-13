// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useRef, useEffect } from 'react';
import { ShapeNode } from '@reifydb/core';
import { useConnection } from './use-connection';
import type { ConnectionConfig } from '../connection/connection';

export interface SubscriptionExecutorOptions {
    connection_config?: ConnectionConfig;
}

export interface ChangeEvent<T> {
    operation: 'INSERT' | 'UPDATE' | 'REMOVE';
    rows: T[];
    timestamp: number;
}

export interface SubscriptionState<T> {
    data: T[];
    changes: ChangeEvent<T>[];
    is_subscribed: boolean;
    is_subscribing: boolean;
    error: string | undefined;
    subscription_id: string | undefined;
}

export function useSubscriptionExecutor<T = any>(
    options?: SubscriptionExecutorOptions
) {
    const { client } = useConnection(options?.connection_config);

    const [state, setState] = useState<SubscriptionState<T>>({
        data: [],
        changes: [],
        is_subscribed: false,
        is_subscribing: false,
        error: undefined,
        subscription_id: undefined
    });

    // Use a ref for client to avoid recreating callbacks when client changes
    const client_ref = useRef(client);

    const subscriptionIdRef = useRef<string | undefined>(undefined);
    const rql_ref = useRef<string | undefined>(undefined);
    const params_ref = useRef<any>(undefined);
    const shape_ref = useRef<ShapeNode | undefined>(undefined);

    // Keep client_ref in sync with client
    useEffect(() => {
        client_ref.current = client;
    }, [client]);

    // Helper to add change event
    const add_change_event = useCallback((
        operation: 'INSERT' | 'UPDATE' | 'REMOVE',
        rows: T[]
    ) => {
        setState(prev => {
            const new_change: ChangeEvent<T> = {
                operation,
                rows,
                timestamp: Date.now()
            };

            const new_changes = [...prev.changes, new_change];

            return {
                ...prev,
                data: prev.data,
                changes: new_changes
            };
        });
    }, []);

    // Separate callbacks for each operation type
    const handle_insert = useCallback((rows: T[]) => {
        add_change_event('INSERT', rows);
    }, [add_change_event]);

    const handle_update = useCallback((rows: T[]) => {
        add_change_event('UPDATE', rows);
    }, [add_change_event]);

    const handle_remove = useCallback((rows: T[]) => {
        add_change_event('REMOVE', rows);
    }, [add_change_event]);

    const subscribe = useCallback(async (
        rql: string,
        params?: any,
        shape?: ShapeNode
    ) => {
        const current_client = client_ref.current;
        if (!current_client) {
            setState(prev => ({ ...prev, error: 'Client not connected' }));
            return;
        }

        if (!('subscribe' in current_client)) {
            setState(prev => ({ ...prev, error: 'Subscriptions require a WebSocket connection' }));
            return;
        }

        // Store refs for reconnection
        rql_ref.current = rql;
        params_ref.current = params;
        shape_ref.current = shape;

        setState(prev => ({
            ...prev,
            is_subscribing: true,
            error: undefined
        }));

        try {
            const sub_id = await current_client.subscribe(rql, params, shape, {
                on_insert: handle_insert,
                on_update: handle_update,
                on_remove: handle_remove
            });

            subscriptionIdRef.current = sub_id;
            setState(prev => ({
                ...prev,
                is_subscribing: false,
                is_subscribed: true,
                subscription_id: sub_id
            }));
        } catch (err: any) {
            setState(prev => ({
                ...prev,
                is_subscribing: false,
                error: err.message || 'Subscription failed'
            }));
        }
    }, [handle_insert, handle_update, handle_remove]);

    const unsubscribe = useCallback(async () => {
        const current_client = client_ref.current;
        if (!current_client || !subscriptionIdRef.current) return;
        if (!('unsubscribe' in current_client)) return;

        try {
            await current_client.unsubscribe(subscriptionIdRef.current);
            subscriptionIdRef.current = undefined;
            rql_ref.current = undefined;
            params_ref.current = undefined;
            shape_ref.current = undefined;

            setState(prev => ({
                ...prev,
                is_subscribed: false,
                subscription_id: undefined
            }));
        } catch (err: any) {
            setState(prev => ({
                ...prev,
                error: err.message || 'Unsubscribe failed'
            }));
        }
    }, []);

    const clear_changes = useCallback(() => {
        setState(prev => ({ ...prev, changes: [] }));
    }, []);

    const clear_data = useCallback(() => {
        setState(prev => ({ ...prev, data: [] }));
    }, []);

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            if (subscriptionIdRef.current && client_ref.current && 'unsubscribe' in client_ref.current) {
                client_ref.current.unsubscribe(subscriptionIdRef.current).catch(console.error);
            }
        };
    }, []);

    return {
        state,
        subscribe,
        unsubscribe,
        clear_changes,
        clear_data
    };
}
