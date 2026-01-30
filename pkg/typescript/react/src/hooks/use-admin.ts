// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {useEffect, useMemo} from 'react';
import {SchemaNode, InferSchema} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useAdminExecutor, type AdminResult, type AdminExecutorOptions} from './use-admin-executor';

export interface AdminOptions extends AdminExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

// Single admin hook - returns a single result
export function useAdminOne<S extends SchemaNode = any>(
    statement: string,
    params?: any,
    schema?: S,
    options?: AdminOptions
): {
    isExecuting: boolean;
    result: AdminResult<S extends SchemaNode ? InferSchema<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        admin
    } = useAdminExecutor<S extends SchemaNode ? InferSchema<S> : any>(options);

    useEffect(() => {
        // Pass schema as array for the executor
        const schemas = schema ? [schema] : undefined;
        admin(statement, params, schemas);
    }, [statement, params, admin]);

    // Extract first result for single admin convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {isExecuting, result, error};
}

// Multiple admin hook - returns multiple results
export function useAdminMany<S extends readonly SchemaNode[] = readonly SchemaNode[]>(
    statements: string | string[],
    params?: any,
    schemas?: S,
    options?: AdminOptions
): {
    isExecuting: boolean;
    results: AdminResult<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        admin
    } = useAdminExecutor<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>(options);

    useEffect(() => {
        admin(statements, params, schemas);
    }, [statements, params, admin]);

    return {isExecuting, results, error};
}
