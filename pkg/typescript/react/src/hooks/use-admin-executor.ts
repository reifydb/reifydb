// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef, useEffect} from 'react';
import {Column, SchemaNode} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useConnection} from './use-connection';

export interface AdminResult<T = any> {
    columns: Column[];
    rows: T[];
    executionTimeMs: number;
    rowsAffected?: number;
}

export interface AdminState<T = any> {
    isExecuting: boolean;
    results: AdminResult<T>[] | undefined;
    error: string | undefined;
    executionTime: number | undefined;
}

export interface AdminExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

export function useAdminExecutor<T = any>(options?: AdminExecutorOptions) {
    const {client} = useConnection(options?.connectionConfig);

    const [state, setState] = useState<AdminState<T>>({
        isExecuting: false,
        results: undefined,
        error: undefined,
        executionTime: undefined,
    });

    // Stable refs so the callback never recreates
    const clientRef = useRef(client);
    clientRef.current = client;

    const isMountedRef = useRef(false);
    useEffect(() => {
        isMountedRef.current = true;
        return () => { isMountedRef.current = false; };
    }, []);

    // Counter to detect superseded executions
    const executionIdRef = useRef(0);

    // Stash pending call if client isn't ready yet
    const pendingRef = useRef<{statements: string | string[], params?: any, schemas?: readonly SchemaNode[]} | null>(null);

    const admin = useCallback(
        (statements: string | string[], params?: any, schemas?: readonly SchemaNode[]): Promise<void> => {
            const currentClient = clientRef.current;
            // If no client yet, stash the request for replay when client connects
            if (!currentClient) {
                pendingRef.current = {statements, params, schemas};
                setState(prev => ({...prev, isExecuting: true, error: undefined}));
                return Promise.resolve();
            }

            pendingRef.current = null;
            const thisExecution = ++executionIdRef.current;

            setState(prev => ({...prev, isExecuting: true, error: undefined}));

            const startTime = Date.now();

            return (async () => {
                try {
                    const frameResults = await currentClient.admin(statements, params || null, schemas || []) || [];

                    // If this execution was superseded by a newer one, discard results
                    if (executionIdRef.current !== thisExecution) return;

                    const executionTime = Date.now() - startTime;

                    // Process each frame into a AdminResult
                    const results: AdminResult<T>[] = frameResults.map((frame: any) => {
                        if (Array.isArray(frame) && frame.length > 0) {
                            const firstRow = frame[0];
                            let columns: Column[] = [];

                            // Check if we have Value objects or plain objects
                            const hasValueObjects = firstRow && typeof firstRow === 'object' &&
                                Object.values(firstRow).some(v => v && typeof v === 'object' && 'type' in v);

                            if (hasValueObjects) {
                                columns = Object.keys(firstRow).map((key) => {
                                    const value = firstRow[key];
                                    const dataType = value?.type || 'Utf8';
                                    return {
                                        name: key,
                                        type: dataType,
                                        payload: [],
                                    };
                                });
                            } else {
                                columns = Object.keys(firstRow).map((key) => ({
                                    name: key,
                                    type: 'Utf8',
                                    payload: [],
                                }));
                            }

                            return {
                                columns,
                                rows: frame as T[],
                                executionTimeMs: executionTime,
                            };
                        } else {
                            return {
                                columns: [],
                                rows: [],
                                executionTimeMs: executionTime,
                                rowsAffected: typeof frame === 'number' ? frame : undefined,
                            };
                        }
                    });

                    if (!isMountedRef.current) return;
                    setState({
                        isExecuting: false,
                        results,
                        error: undefined,
                        executionTime,
                    });
                } catch (err) {
                    if (executionIdRef.current !== thisExecution) return;

                    const executionTime = Date.now() - startTime;
                    let errorMessage = 'Admin execution failed';

                    if (err instanceof Error) {
                        errorMessage = err.message;
                    } else if (typeof err === 'string') {
                        errorMessage = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        errorMessage = (err as { message: string }).message;
                    }

                    console.error('Admin execution failed:', errorMessage);

                    if (!isMountedRef.current) return;
                    setState(prev => ({
                        ...prev,
                        isExecuting: false,
                        error: errorMessage,
                        executionTime,
                    }));

                    throw err;
                }
            })();
        },
        []  // stable — never recreates
    );

    // Replay pending request when client becomes available
    useEffect(() => {
        if (client && pendingRef.current) {
            const {statements, params, schemas} = pendingRef.current;
            admin(statements, params, schemas);
        }
    }, [client, admin]);

    const cancelAdmin = useCallback(() => {
        // Bump execution ID so any in-flight request is ignored on completion
        executionIdRef.current++;
        setState((prev) => ({
            ...prev,
            isExecuting: false,
            error: 'Admin cancelled',
        }));
    }, []);

    return {
        isExecuting: state.isExecuting,
        results: state.results,
        error: state.error,
        executionTime: state.executionTime,
        admin,
        cancelAdmin,
    };
}
