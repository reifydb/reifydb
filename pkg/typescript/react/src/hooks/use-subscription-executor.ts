// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useRef, useEffect } from 'react';
import { SchemaNode } from '@reifydb/core';
import { useConnection } from './use-connection';
import type { ConnectionConfig } from '../connection/connection';

export interface SubscriptionExecutorOptions {
    connectionConfig?: ConnectionConfig;
    maxChanges?: number;       // Max change events to retain (default: 50)
}

export interface ChangeEvent<T> {
    operation: 'INSERT' | 'UPDATE' | 'REMOVE';
    rows: T[];
    timestamp: number;
}

export interface SubscriptionState<T> {
    data: T[];
    changes: ChangeEvent<T>[];
    isSubscribed: boolean;
    isSubscribing: boolean;
    error: string | undefined;
    subscriptionId: string | undefined;
}

export function useSubscriptionExecutor<T = any>(
    options?: SubscriptionExecutorOptions
) {
    const { client } = useConnection(options?.connectionConfig);

    const [state, setState] = useState<SubscriptionState<T>>({
        data: [],
        changes: [],
        isSubscribed: false,
        isSubscribing: false,
        error: undefined,
        subscriptionId: undefined
    });

    // Use a ref for client to avoid recreating callbacks when client changes
    const clientRef = useRef(client);

    const subscriptionIdRef = useRef<string | undefined>(undefined);
    const queryRef = useRef<string | undefined>(undefined);
    const paramsRef = useRef<any>(undefined);
    const schemaRef = useRef<SchemaNode | undefined>(undefined);

    // Keep clientRef in sync with client
    useEffect(() => {
        clientRef.current = client;
    }, [client]);

    // Helper to add change event and accumulate data
    const maxChangesOption = options?.maxChanges;
    const addChangeEvent = useCallback((
        operation: 'INSERT' | 'UPDATE' | 'REMOVE',
        rows: T[]
    ) => {
        setState(prev => {
            const newChange: ChangeEvent<T> = {
                operation,
                rows,
                timestamp: Date.now()
            };

            const maxChanges = maxChangesOption ?? 50;
            const newChanges = [...prev.changes, newChange].slice(-maxChanges);

            return {
                ...prev,
                data: prev.data,
                changes: newChanges
            };
        });
    }, [maxChangesOption]);

    // Separate callbacks for each operation type
    const handleInsert = useCallback((rows: T[]) => {
        addChangeEvent('INSERT', rows);
    }, [addChangeEvent]);

    const handleUpdate = useCallback((rows: T[]) => {
        addChangeEvent('UPDATE', rows);
    }, [addChangeEvent]);

    const handleRemove = useCallback((rows: T[]) => {
        addChangeEvent('REMOVE', rows);
    }, [addChangeEvent]);

    const subscribe = useCallback(async (
        query: string,
        params?: any,
        schema?: SchemaNode
    ) => {
        const currentClient = clientRef.current;
        if (!currentClient) {
            setState(prev => ({ ...prev, error: 'Client not connected' }));
            return;
        }

        // Store refs for reconnection
        queryRef.current = query;
        paramsRef.current = params;
        schemaRef.current = schema;

        setState(prev => ({
            ...prev,
            isSubscribing: true,
            error: undefined
        }));

        try {
            const subId = await currentClient.subscribe(query, params, schema, {
                onInsert: handleInsert,
                onUpdate: handleUpdate,
                onRemove: handleRemove
            });

            subscriptionIdRef.current = subId;
            setState(prev => ({
                ...prev,
                isSubscribing: false,
                isSubscribed: true,
                subscriptionId: subId
            }));
        } catch (err: any) {
            setState(prev => ({
                ...prev,
                isSubscribing: false,
                error: err.message || 'Subscription failed'
            }));
        }
    }, [handleInsert, handleUpdate, handleRemove]);

    const unsubscribe = useCallback(async () => {
        const currentClient = clientRef.current;
        if (!currentClient || !subscriptionIdRef.current) return;

        try {
            await currentClient.unsubscribe(subscriptionIdRef.current);
            subscriptionIdRef.current = undefined;
            queryRef.current = undefined;
            paramsRef.current = undefined;
            schemaRef.current = undefined;

            setState(prev => ({
                ...prev,
                isSubscribed: false,
                subscriptionId: undefined
            }));
        } catch (err: any) {
            setState(prev => ({
                ...prev,
                error: err.message || 'Unsubscribe failed'
            }));
        }
    }, []);

    const clearChanges = useCallback(() => {
        setState(prev => ({ ...prev, changes: [] }));
    }, []);

    const clearData = useCallback(() => {
        setState(prev => ({ ...prev, data: [] }));
    }, []);

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            if (subscriptionIdRef.current && clientRef.current) {
                clientRef.current.unsubscribe(subscriptionIdRef.current).catch(console.error);
            }
        };
    }, []);

    return {
        state,
        subscribe,
        unsubscribe,
        clearChanges,
        clearData
    };
}
