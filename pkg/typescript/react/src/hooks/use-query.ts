// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useEffect, useMemo} from 'react';
import {ShapeNode, InferShape} from '@reifydb/core';
import {ConnectionConfig} from '../connection/connection';
import {useQueryExecutor, type QueryResult, type QueryExecutorOptions} from './use-query-executor';

export interface QueryOptions extends QueryExecutorOptions {
    connection_config?: ConnectionConfig;
}

// Single query hook - returns a single result
export function useQueryOne<S extends ShapeNode = any>(
    rql: string,
    params?: any,
    shape?: S,
    options?: QueryOptions
): {
    is_executing: boolean;
    result: QueryResult<S extends ShapeNode ? InferShape<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        is_executing,
        results,
        error,
        query
    } = useQueryExecutor<S extends ShapeNode ? InferShape<S> : any>(options);

    useEffect(() => {
        // Pass shape as array for the executor
        const shapes = shape ? [shape] : undefined;
        query(rql, params, shapes);
    }, [rql, params, query]);

    // Extract first result for single query convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {is_executing, result, error};
}

// Multiple query hook - returns multiple results
export function useQueryMany<S extends readonly ShapeNode[] = readonly ShapeNode[]>(
    statements: string | string[],
    params?: any,
    shapes?: S,
    options?: QueryOptions
): {
    is_executing: boolean;
    results: QueryResult<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        is_executing,
        results,
        error,
        query
    } = useQueryExecutor<S extends readonly ShapeNode[] ? InferShape<S[number]> : any>(options);

    useEffect(() => {
        query(statements, params, shapes);
    }, [statements, params, query]);

    return {is_executing, results, error};
}