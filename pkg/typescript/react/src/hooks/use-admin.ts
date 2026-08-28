// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {useEffect, useMemo} from 'react';
import {ShapeNode, InferShape} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useAdminExecutor, type AdminResult, type AdminExecutorOptions} from './use-admin-executor';

export interface AdminOptions extends AdminExecutorOptions {
    connectionConfig?: ConnectionConfig;
}

// Single admin hook - returns a single result
export function useAdminOne<S extends ShapeNode = any>(
    rql: string,
    params?: any,
    shape?: S,
    options?: AdminOptions
): {
    isExecuting: boolean;
    result: AdminResult<S extends ShapeNode ? InferShape<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        admin
    } = useAdminExecutor<S extends ShapeNode ? InferShape<S> : any>(options);

    useEffect(() => {
        // Pass shape as array for the executor
        const shapes = shape ? [shape] : undefined;
        admin(rql, params, shapes);
    }, [rql, params, admin]);

    // Extract first result for single admin convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {isExecuting, result, error};
}

// Multiple admin hook - returns multiple results
export function useAdminMany<S extends readonly ShapeNode[] = readonly ShapeNode[]>(
    rql: string,
    params?: any,
    shapes?: S,
    options?: AdminOptions
): {
    isExecuting: boolean;
    results: AdminResult<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        admin
    } = useAdminExecutor<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>(options);

    useEffect(() => {
        admin(rql, params, shapes);
    }, [rql, params, admin]);

    return {isExecuting, results, error};
}
