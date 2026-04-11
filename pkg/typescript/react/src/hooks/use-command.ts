// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useEffect, useMemo} from 'react';
import {ShapeNode, InferShape} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useCommandExecutor, type CommandResult, type CommandExecutorOptions} from './use-command-executor';

export interface CommandOptions extends CommandExecutorOptions {
    connection_config?: ConnectionConfig;
}

// Single command hook - returns a single result
export function useCommandOne<S extends ShapeNode = any>(
    statement: string,
    params?: any,
    shape?: S,
    options?: CommandOptions
): {
    is_executing: boolean;
    result: CommandResult<S extends ShapeNode ? InferShape<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        is_executing,
        results,
        error,
        command
    } = useCommandExecutor<S extends ShapeNode ? InferShape<S> : any>(options);

    useEffect(() => {
        // Pass shape as array for the executor
        const shapes = shape ? [shape] : undefined;
        command(statement, params, shapes);
    }, [statement, params, command]);

    // Extract first result for single command convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {is_executing, result, error};
}

// Multiple command hook - returns multiple results
export function useCommandMany<S extends readonly ShapeNode[] = readonly ShapeNode[]>(
    statements: string | string[],
    params?: any,
    shapes?: S,
    options?: CommandOptions
): {
    is_executing: boolean;
    results: CommandResult<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        is_executing,
        results,
        error,
        command
    } = useCommandExecutor<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>(options);

    useEffect(() => {
        command(statements, params, shapes);
    }, [statements, params, command]);

    return {is_executing, results, error};
}