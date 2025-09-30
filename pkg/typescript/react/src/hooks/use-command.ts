import {useEffect, useMemo} from 'react';
import {SchemaNode, InferSchema} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useCommandExecutor, type CommandResult, type CommandExecutorOptions} from './use-command-executor';

export interface CommandOptions extends CommandExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

// Single command hook - returns a single result
export function useCommandOne<S extends SchemaNode = any>(
    statement: string,
    params?: any,
    schema?: S,
    options?: CommandOptions
): {
    isExecuting: boolean;
    result: CommandResult<S extends SchemaNode ? InferSchema<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        command
    } = useCommandExecutor<S extends SchemaNode ? InferSchema<S> : any>(options);

    useEffect(() => {
        // Pass schema as array for the executor
        const schemas = schema ? [schema] : undefined;
        command(statement, params, schemas);
    }, [statement, params, command]);

    // Extract first result for single command convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {isExecuting, result, error};
}

// Multiple command hook - returns multiple results
export function useCommandMany<S extends readonly SchemaNode[] = readonly SchemaNode[]>(
    statements: string | string[],
    params?: any,
    schemas?: S,
    options?: CommandOptions
): {
    isExecuting: boolean;
    results: CommandResult<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        command
    } = useCommandExecutor<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>(options);

    useEffect(() => {
        command(statements, params, schemas);
    }, [statements, params, command]);

    return {isExecuting, results, error};
}