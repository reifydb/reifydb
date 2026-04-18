// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useState, useCallback, useRef, useEffect} from 'react';
import {Column, ShapeNode} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useConnection} from './use-connection';

export interface CommandResult<T = any> {
    columns: Column[];
    rows: T[];
    execution_time_ms: number;
    rows_affected?: number;
}

export interface CommandState<T = any> {
    is_executing: boolean;
    results: CommandResult<T>[] | undefined;
    error: string | undefined;
    execution_time: number | undefined;
}

export interface CommandExecutorOptions {
    connection_config?: ConnectionConfig;
}

export function useCommandExecutor<T = any>(options?: CommandExecutorOptions) {
    const {client} = useConnection(options?.connection_config);

    const [state, setState] = useState<CommandState<T>>({
        is_executing: false,
        results: undefined,
        error: undefined,
        execution_time: undefined,
    });

    const client_ref = useRef(client);
    client_ref.current = client;

    const is_mounted_ref = useRef(false);
    useEffect(() => {
        is_mounted_ref.current = true;
        return () => { is_mounted_ref.current = false; };
    }, []);

    const execution_id_ref = useRef(0);
    const pending_ref = useRef<{rql: string, params?: any, shapes?: readonly ShapeNode[]} | null>(null);

    const command = useCallback(
        (rql: string, params?: any, shapes?: readonly ShapeNode[]): Promise<void> => {
            const current_client = client_ref.current;

            if (!current_client) {
                pending_ref.current = {rql, params, shapes};
                setState(prev => ({...prev, is_executing: true, error: undefined}));
                return Promise.resolve();
            }

            pending_ref.current = null;
            const this_execution = ++execution_id_ref.current;

            setState(prev => ({...prev, is_executing: true, error: undefined}));

            const start_time = Date.now();

            return (async () => {
                try {
                    const frame_results = await current_client.command(rql, params || null, shapes || []) || [];

                    if (execution_id_ref.current !== this_execution) return;

                    const execution_time = Date.now() - start_time;

                    const results: CommandResult<T>[] = frame_results.map((frame: any) => {
                        if (Array.isArray(frame) && frame.length > 0) {
                            const first_row = frame[0];
                            let columns: Column[] = [];

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
                    let error_message = 'Command execution failed';

                    if (err instanceof Error) {
                        error_message = err.message;
                    } else if (typeof err === 'string') {
                        error_message = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        error_message = (err as { message: string }).message;
                    }

                    console.error('Command execution failed:', error_message);

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
        []
    );

    useEffect(() => {
        if (client && pending_ref.current) {
            const {rql, params, shapes} = pending_ref.current;
            command(rql, params, shapes);
        }
    }, [client, command]);

    const cancel_command = useCallback(() => {
        execution_id_ref.current++;
        setState((prev) => ({
            ...prev,
            is_executing: false,
            error: 'Command cancelled',
        }));
    }, []);

    return {
        is_executing: state.is_executing,
        results: state.results,
        error: state.error,
        execution_time: state.execution_time,
        command,
        cancel_command,
    };
}
