// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef, useEffect} from 'react';
import {Column, SchemaNode} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useConnection} from './use-connection';

export interface QueryResult<T = any> {
    columns: Column[];
    rows: T[];
    executionTimeMs: number;
    rowsAffected?: number;
}

export interface QueryState<T = any> {
    isExecuting: boolean;
    results: QueryResult<T>[] | undefined;
    error: string | undefined;
    executionTime: number | undefined;
}

export interface QueryExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

export function useQueryExecutor<T = any>(options?: QueryExecutorOptions) {
    const {client} = useConnection(options?.connectionConfig);

    const [state, setState] = useState<QueryState<T>>({
        isExecuting: false,
        results: undefined,
        error: undefined,
        executionTime: undefined,
    });

    const clientRef = useRef(client);
    clientRef.current = client;

    const isMountedRef = useRef(true);
    useEffect(() => {
        return () => { isMountedRef.current = false; };
    }, []);

    const executionIdRef = useRef(0);
    const pendingRef = useRef<{statements: string | string[], params?: any, schemas?: readonly SchemaNode[]} | null>(null);

    const query = useCallback(
        (statements: string | string[], params?: any, schemas?: readonly SchemaNode[]): Promise<void> => {
            const currentClient = clientRef.current;

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
                    const frameResults = await currentClient.query(statements, params || null, schemas || []) || [];

                    if (executionIdRef.current !== thisExecution) return;

                    const executionTime = Date.now() - startTime;

                    const results: QueryResult<T>[] = frameResults.map((frame: any) => {
                        if (Array.isArray(frame) && frame.length > 0) {
                            const firstRow = frame[0];
                            let columns: Column[] = [];

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
                    let errorMessage = 'Query execution failed';

                    if (err instanceof Error) {
                        errorMessage = err.message;
                    } else if (typeof err === 'string') {
                        errorMessage = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        errorMessage = (err as { message: string }).message;
                    }

                    console.error('Query execution failed:', errorMessage);

                    if (!isMountedRef.current) return;
                    setState(prev => ({
                        ...prev,
                        isExecuting: false,
                        error: errorMessage,
                        executionTime,
                    }));

                }
            })();
        },
        []
    );

    useEffect(() => {
        if (client && pendingRef.current) {
            const {statements, params, schemas} = pendingRef.current;
            query(statements, params, schemas);
        }
    }, [client, query]);

    const cancelQuery = useCallback(() => {
        executionIdRef.current++;
        setState((prev) => ({
            ...prev,
            isExecuting: false,
            error: 'Query cancelled',
        }));
    }, []);

    return {
        isExecuting: state.isExecuting,
        results: state.results,
        error: state.error,
        executionTime: state.executionTime,
        query,
        cancelQuery,
    };
}
