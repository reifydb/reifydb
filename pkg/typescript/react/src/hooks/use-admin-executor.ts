// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef, useEffect} from 'react';
import {Column, ShapeNode} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useConnection} from './use-connection';

export interface AdminResult<T = any> {
    columns: Column[];
    rows: T[];
    execution_time_ms: number;
    rows_affected?: number;
}

export interface AdminState<T = any> {
    is_executing: boolean;
    results: AdminResult<T>[] | undefined;
    error: string | undefined;
    execution_time: number | undefined;
}

export interface AdminExecutorOptions {
    connection_config?: ConnectionConfig;
}

export function useAdminExecutor<T = any>(options?: AdminExecutorOptions) {
    const {client} = useConnection(options?.connection_config);

    const [state, setState] = useState<AdminState<T>>({
        is_executing: false,
        results: undefined,
        error: undefined,
        execution_time: undefined,
    });

    // Stable refs so the callback never recreates
    const client_ref = useRef(client);
    client_ref.current = client;

    const is_mounted_ref = useRef(false);
    useEffect(() => {
        is_mounted_ref.current = true;
        return () => { is_mounted_ref.current = false; };
    }, []);

    // Counter to detect superseded executions
    const execution_id_ref = useRef(0);

    // Stash pending call if client isn't ready yet
    const pending_ref = useRef<{statements: string | string[], params?: any, shapes?: readonly ShapeNode[]} | null>(null);

    const admin = useCallback(
        (statements: string | string[], params?: any, shapes?: readonly ShapeNode[]): Promise<void> => {
            const current_client = client_ref.current;
            // If no client yet, stash the request for replay when client connects
            if (!current_client) {
                pending_ref.current = {statements, params, shapes};
                setState(prev => ({...prev, is_executing: true, error: undefined}));
                return Promise.resolve();
            }

            pending_ref.current = null;
            const this_execution = ++execution_id_ref.current;

            setState(prev => ({...prev, is_executing: true, error: undefined}));

            const start_time = Date.now();

            return (async () => {
                try {
                    const frame_results = await current_client.admin(statements, params || null, shapes || []) || [];

                    // If this execution was superseded by a newer one, discard results
                    if (execution_id_ref.current !== this_execution) return;

                    const execution_time = Date.now() - start_time;

                    // Process each frame into a AdminResult
                    const results: AdminResult<T>[] = frame_results.map((frame: any) => {
                        if (Array.isArray(frame) && frame.length > 0) {
                            const first_row = frame[0];
                            let columns: Column[] = [];

                            // Check if we have Value objects or plain objects
                            const has_value_objects = first_row && typeof first_row === 'object' &&
                                Object.values(first_row).some(v => v && typeof v === 'object' && 'type' in v);

                            if (has_value_objects) {
                                columns = Object.keys(first_row).map((key) => {
                                    const value = first_row[key];
                                    const data_type = value?.type || 'Utf8';
                                    return {
                                        name: key,
                                        type: data_type,
                                        payload: [],
                                    };
                                });
                            } else {
                                columns = Object.keys(first_row).map((key) => ({
                                    name: key,
                                    type: 'Utf8',
                                    payload: [],
                                }));
                            }

                            return {
                                columns,
                                rows: frame as T[],
                                execution_time_ms: execution_time,
                            };
                        } else {
                            return {
                                columns: [],
                                rows: [],
                                execution_time_ms: execution_time,
                                rows_affected: typeof frame === 'number' ? frame : undefined,
                            };
                        }
                    });

                    if (!is_mounted_ref.current) return;
                    setState({
                        is_executing: false,
                        results,
                        error: undefined,
                        execution_time,
                    });
                } catch (err) {
                    if (execution_id_ref.current !== this_execution) return;

                    const execution_time = Date.now() - start_time;
                    let error_message = 'Admin execution failed';

                    if (err instanceof Error) {
                        error_message = err.message;
                    } else if (typeof err === 'string') {
                        error_message = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        error_message = (err as { message: string }).message;
                    }

                    console.error('Admin execution failed:', error_message);

                    if (!is_mounted_ref.current) return;
                    setState(prev => ({
                        ...prev,
                        is_executing: false,
                        error: error_message,
                        execution_time,
                    }));

                }
            })();
        },
        []  // stable — never recreates
    );

    // Replay pending request when client becomes available
    useEffect(() => {
        if (client && pending_ref.current) {
            const {statements, params, shapes} = pending_ref.current;
            admin(statements, params, shapes);
        }
    }, [client, admin]);

    const cancel_admin = useCallback(() => {
        // Bump execution ID so any in-flight request is ignored on completion
        execution_id_ref.current++;
        setState((prev) => ({
            ...prev,
            is_executing: false,
            error: 'Admin cancelled',
        }));
    }, []);

    return {
        is_executing: state.is_executing,
        results: state.results,
        error: state.error,
        execution_time: state.execution_time,
        admin,
        cancel_admin,
    };
}
