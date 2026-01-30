// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef} from 'react';
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

    const abortControllerRef = useRef<AbortController | null>(null);

    const admin = useCallback(
        (statements: string | string[], params?: any, schemas?: readonly SchemaNode[]): void => {
            // Cancel any ongoing admin for THIS instance only
            if (abortControllerRef.current) {
                abortControllerRef.current.abort();
            }
            abortControllerRef.current = new AbortController();

            setState({
                isExecuting: true,
                results: undefined,
                error: undefined,
                executionTime: undefined,
            });

            const startTime = Date.now();

            (async () => {
                try {
                    // Call client.admin which returns FrameResults (array of frames)
                    // Commands and queries both use the same admin method
                    const frameResults = await client?.admin(statements, params || null, schemas || []) || [];

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
                                // We have Value objects - extract type info
                                columns = Object.keys(firstRow).map((key) => {
                                    const value = firstRow[key];
                                    const dataType = value?.type || 'Utf8';
                                    return {
                                        name: key,
                                        type: dataType,
                                        data: [],
                                    };
                                });
                            } else {
                                // Plain objects from schema conversion
                                columns = Object.keys(firstRow).map((key) => ({
                                    name: key,
                                    type: 'Utf8', // Default type for plain objects
                                    data: [],
                                }));
                            }

                            return {
                                columns,
                                rows: frame as T[],
                                executionTimeMs: executionTime,
                            };
                        } else {
                            // Empty result or rows affected
                            return {
                                columns: [],
                                rows: [],
                                executionTimeMs: executionTime,
                                rowsAffected: typeof frame === 'number' ? frame : undefined,
                            };
                        }
                    });

                    setState({
                        isExecuting: false,
                        results,
                        error: undefined,
                        executionTime,
                    });
                } catch (err) {
                    const executionTime = Date.now() - startTime;
                    let errorMessage = 'Admin execution failed';

                    if (err instanceof Error) {
                        errorMessage = err.message;
                    } else if (typeof err === 'string') {
                        errorMessage = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        errorMessage = (err as { message: string }).message;
                    }

                    setState({
                        isExecuting: false,
                        results: undefined,
                        error: errorMessage,
                        executionTime,
                    });
                } finally {
                    abortControllerRef.current = null;
                }
            })();
        },
        [client]
    );

    const cancelAdmin = useCallback(() => {
        if (abortControllerRef.current) {
            abortControllerRef.current.abort();
            setState((prev) => ({
                ...prev,
                isExecuting: false,
                error: 'Admin cancelled',
            }));
        }
    }, []);

    return {
        // State
        isExecuting: state.isExecuting,
        results: state.results,
        error: state.error,
        executionTime: state.executionTime,

        // Actions
        admin,
        cancelAdmin,
    };
}
