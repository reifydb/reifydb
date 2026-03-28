// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef, useEffect} from 'react';
import {Column, SchemaNode} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useConnection} from './use-connection';

export interface CommandResult<T = any> {
    columns: Column[];
    rows: T[];
    executionTimeMs: number;
    rowsAffected?: number;
}

export interface CommandState<T = any> {
    isExecuting: boolean;
    results: CommandResult<T>[] | undefined;
    error: string | undefined;
    executionTime: number | undefined;
}

export interface CommandExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

export function useCommandExecutor<T = any>(options?: CommandExecutorOptions) {
    const {client} = useConnection(options?.connectionConfig);

    const [state, setState] = useState<CommandState<T>>({
        isExecuting: false,
        results: undefined,
        error: undefined,
        executionTime: undefined,
    });

    const clientRef = useRef(client);
    clientRef.current = client;

    const isMountedRef = useRef(false);
    useEffect(() => {
        isMountedRef.current = true;
        return () => { isMountedRef.current = false; };
    }, []);

    const executionIdRef = useRef(0);
    const pendingRef = useRef<{statements: string | string[], params?: any, schemas?: readonly SchemaNode[]} | null>(null);

    const command = useCallback(
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
                    const frameResults = await currentClient.command(statements, params || null, schemas || []) || [];

                    if (executionIdRef.current !== thisExecution) return;

                    const executionTime = Date.now() - startTime;

                    const results: CommandResult<T>[] = frameResults.map((frame: any) => {
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
                    let errorMessage = 'Command execution failed';

                    if (err instanceof Error) {
                        errorMessage = err.message;
                    } else if (typeof err === 'string') {
                        errorMessage = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        errorMessage = (err as { message: string }).message;
                    }

                    console.error('Command execution failed:', errorMessage);

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
        []
    );

    useEffect(() => {
        if (client && pendingRef.current) {
            const {statements, params, schemas} = pendingRef.current;
            command(statements, params, schemas);
        }
    }, [client, command]);

    const cancelCommand = useCallback(() => {
        executionIdRef.current++;
        setState((prev) => ({
            ...prev,
            isExecuting: false,
            error: 'Command cancelled',
        }));
    }, []);

    return {
        isExecuting: state.isExecuting,
        results: state.results,
        error: state.error,
        executionTime: state.executionTime,
        command,
        cancelCommand,
    };
}
